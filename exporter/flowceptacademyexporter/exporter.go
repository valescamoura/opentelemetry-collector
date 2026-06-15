// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package flowceptacademyexporter // import "go.opentelemetry.io/collector/exporter/flowceptacademyexporter"

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	statusFinished = "FINISHED"
	statusError    = "ERROR"
)

var academyIDPattern = regexp.MustCompile(`\b(?:AgentID|AgentId|UserID|UserId)<([^>]+)>`)

type publisher interface {
	Publish(ctx context.Context, channel string, payload []byte) error
	Close() error
}

type redisPublisher struct {
	client *redis.Client
}

func newRedisPublisher(cfg Config) (*redisPublisher, error) {
	var opts *redis.Options
	var err error
	if cfg.RedisURL != "" {
		opts, err = redis.ParseURL(cfg.RedisURL)
		if err != nil {
			return nil, err
		}
	} else {
		opts = &redis.Options{
			Addr:     cfg.RedisAddr,
			DB:       cfg.RedisDB,
			Username: cfg.RedisUsername,
			Password: cfg.RedisPassword,
		}
	}
	return &redisPublisher{client: redis.NewClient(opts)}, nil
}

func (p *redisPublisher) Publish(ctx context.Context, channel string, payload []byte) error {
	return p.client.Publish(ctx, channel, payload).Err()
}

func (p *redisPublisher) Close() error {
	return p.client.Close()
}

type flowceptExporter struct {
	cfg       Config
	publisher publisher

	mu                 sync.Mutex
	publishedWorkflows map[string]struct{}
}

func newFlowceptExporter(cfg Config, publisher publisher) *flowceptExporter {
	return &flowceptExporter{
		cfg:                cfg.normalized(),
		publisher:          publisher,
		publishedWorkflows: make(map[string]struct{}),
	}
}

type spanRecord struct {
	span          ptrace.Span
	resourceAttrs map[string]any
	scopeName     string
}

type traceMapping struct {
	workflowID   string
	workflowName string
	chainSpan    ptrace.Span
	hasChain     bool
}

type agentInfo struct {
	ID   string
	Name string
}

func (e *flowceptExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	records := make([]spanRecord, 0)
	traces := make(map[string]*traceMapping)
	spanKinds := make(map[string]string)
	agentsBySpanID := make(map[string]agentInfo)
	agentsByAgentID := make(map[string]agentInfo)

	resourceSpans := td.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)
		resourceAttrs := rs.Resource().Attributes().AsRaw()
		scopeSpans := rs.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			ss := scopeSpans.At(j)
			scopeName := ss.Scope().Name()
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				traceID := span.TraceID().String()
				record := spanRecord{span: span, resourceAttrs: resourceAttrs, scopeName: scopeName}
				records = append(records, record)

				if _, ok := traces[traceID]; !ok {
					traces[traceID] = &traceMapping{workflowID: traceID}
				}
				attrs := span.Attributes().AsRaw()
				spanKind := stringAttr(attrs, "openinference.span.kind")
				spanKinds[span.SpanID().String()] = spanKind
				if spanKind == "CHAIN" && !traces[traceID].hasChain {
					traces[traceID].workflowID = span.SpanID().String()
					traces[traceID].workflowName = span.Name()
					traces[traceID].chainSpan = span
					traces[traceID].hasChain = true
				}
				if spanKind == "AGENT" {
					agent := agentInfo{
						ID:   cleanAcademyID(stringAttr(attrs, "academy.agent.id")),
						Name: stringAttr(attrs, "agent.name"),
					}
					agentsBySpanID[span.SpanID().String()] = agent
					if agent.ID != "" {
						agentsByAgentID[agent.ID] = agent
					}
				}
			}
		}
	}

	if !e.cfg.DisableWorkflowObjects {
		for traceID, mapping := range traces {
			if mapping.hasChain {
				if err := e.publishWorkflow(ctx, mapping.chainSpan, mapping.workflowID); err != nil {
					return err
				}
			} else {
				if err := e.publishFallbackWorkflow(ctx, traceID); err != nil {
					return err
				}
			}
		}
	}

	for _, record := range records {
		attrs := record.span.Attributes().AsRaw()
		if stringAttr(attrs, "openinference.span.kind") != "TOOL" {
			continue
		}
		workflowID := record.span.TraceID().String()
		workflowName := ""
		if mapping, ok := traces[workflowID]; ok {
			workflowID = mapping.workflowID
			workflowName = mapping.workflowName
		}
		if err := e.publishObject(ctx, e.spanToTask(record.span, record.resourceAttrs, record.scopeName, workflowID, workflowName, spanKinds, agentsBySpanID, agentsByAgentID)); err != nil {
			return err
		}
	}
	return nil
}

func (e *flowceptExporter) shutdown(context.Context) error {
	if e.publisher == nil {
		return nil
	}
	return e.publisher.Close()
}

func (e *flowceptExporter) publishWorkflow(ctx context.Context, span ptrace.Span, workflowID string) error {
	if e.workflowAlreadyPublished(workflowID) {
		return nil
	}

	attrs := span.Attributes().AsRaw()
	safeAttrs := sanitizeMongoKeys(normalizeAcademyIDs(attrs))
	resourceAttrs := map[string]any{}
	workflow := map[string]any{
		"type":          "workflow",
		"workflow_id":   workflowID,
		"name":          span.Name(),
		"adapter_id":    e.cfg.AdapterID,
		"subtype":       "academy.workflow",
		"utc_timestamp": nowUnixSeconds(),
		"started_at":    unixSeconds(span.StartTimestamp()),
		"ended_at":      unixSeconds(span.EndTimestamp()),
		"custom_metadata": map[string]any{
			"source": "opentelemetry",
			"otel": map[string]any{
				"trace_id":       span.TraceID().String(),
				"span_id":        span.SpanID().String(),
				"parent_span_id": span.ParentSpanID().String(),
				"span_kind":      span.Kind().String(),
			},
			"attributes": safeAttrs,
			"events":     sanitizeMongoKeys(normalizeAcademyIDs(eventsAsRaw(span.Events()))),
		},
	}
	if campaignID := e.campaignID(attrs, resourceAttrs); campaignID != "" {
		workflow["campaign_id"] = campaignID
	}
	if used, ok := flowceptIO(attrs, "input.value"); ok {
		workflow["used"] = used
	}
	if generated, ok := flowceptIO(attrs, "output.value"); ok {
		workflow["generated"] = generated
	}
	addParsedMetadata(workflow["custom_metadata"].(map[string]any), attrs)
	return e.publishObject(ctx, workflow)
}

func (e *flowceptExporter) publishFallbackWorkflow(ctx context.Context, traceID string) error {
	if e.workflowAlreadyPublished(traceID) {
		return nil
	}
	return e.publishObject(ctx, map[string]any{
		"type":          "workflow",
		"workflow_id":   traceID,
		"name":          "otel_trace_" + traceID,
		"adapter_id":    e.cfg.AdapterID,
		"subtype":       "otel.trace",
		"utc_timestamp": nowUnixSeconds(),
		"custom_metadata": map[string]any{
			"trace_id": traceID,
			"source":   "opentelemetry",
		},
	})
}

func (e *flowceptExporter) workflowAlreadyPublished(workflowID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.publishedWorkflows[workflowID]; ok {
		return true
	}
	e.publishedWorkflows[workflowID] = struct{}{}
	return false
}

func (e *flowceptExporter) spanToTask(
	span ptrace.Span,
	resourceAttrs map[string]any,
	scopeName string,
	workflowID string,
	workflowName string,
	spanKinds map[string]string,
	agentsBySpanID map[string]agentInfo,
	agentsByAgentID map[string]agentInfo,
) map[string]any {
	attrs := span.Attributes().AsRaw()
	safeAttrs := sanitizeMongoKeys(normalizeAcademyIDs(attrs))
	spanID := span.SpanID().String()
	openInferenceKind := stringAttr(attrs, "openinference.span.kind")
	task := map[string]any{
		"type":          "task",
		"task_id":       spanID,
		"workflow_id":   workflowID,
		"activity_id":   activityIDForSpan(span, attrs),
		"adapter_id":    e.cfg.AdapterID,
		"subtype":       "academy.action",
		"utc_timestamp": nowUnixSeconds(),
		"started_at":    unixSeconds(span.StartTimestamp()),
		"ended_at":      unixSeconds(span.EndTimestamp()),
		"status":        statusForSpan(span),
		"custom_metadata": map[string]any{
			"otel": map[string]any{
				"trace_id":              span.TraceID().String(),
				"span_id":               spanID,
				"parent_span_id":        span.ParentSpanID().String(),
				"span_kind":             span.Kind().String(),
				"instrumentation_scope": scopeName,
			},
			"attributes": safeAttrs,
			"events":     sanitizeMongoKeys(normalizeAcademyIDs(eventsAsRaw(span.Events()))),
			"resource":   sanitizeMongoKeys(normalizeAcademyIDs(resourceAttrs)),
		},
	}
	if workflowName != "" {
		task["workflow_name"] = workflowName
	}
	addTaskResourceFields(task, resourceAttrs)

	parent := span.ParentSpanID().String()
	if parent != "" && spanKinds[parent] != "CHAIN" && spanKinds[parent] != "AGENT" {
		task["parent_task_id"] = parent
		task["dependencies"] = []any{parent}
	}
	if campaignID := e.campaignID(attrs, resourceAttrs); campaignID != "" {
		task["campaign_id"] = campaignID
	}
	agent := agentInfo{
		ID: cleanAcademyID(stringAttr(attrs, "academy.agent.id")),
	}
	if parentAgent, ok := agentsBySpanID[parent]; ok {
		if agent.ID == "" {
			agent.ID = parentAgent.ID
		}
		agent.Name = parentAgent.Name
	}
	if agent.ID != "" {
		task["agent_id"] = agent.ID
	}
	if agent.Name != "" {
		task["agent_name"] = agent.Name
	}
	if sourceAgentID := sourceAgentIDForSpan(openInferenceKind, attrs, span.Events()); sourceAgentID != "" {
		sourceAgentID = cleanAcademyID(sourceAgentID)
		task["source_agent_id"] = sourceAgentID
		if sourceAgent, ok := agentsByAgentID[sourceAgentID]; ok && sourceAgent.Name != "" {
			task["source_agent_name"] = sourceAgent.Name
		}
	}
	if used, ok := flowceptIO(attrs, "input.value"); ok {
		task["used"] = used
	}
	if generated, ok := flowceptIO(attrs, "output.value"); ok {
		task["generated"] = generated
	}
	if messageTag := messageTagForSpan(attrs, span.Events()); messageTag != "" {
		task["group_id"] = messageTag
	}
	if telemetry, ok := telemetryForEvent(span.Events(), "academy.telemetry.start"); ok {
		moveTelemetryRuntime(task["custom_metadata"].(map[string]any), telemetry, "telemetry_runtime_at_start")
		task["telemetry_at_start"] = telemetry
	}
	if telemetry, ok := telemetryForEvent(span.Events(), "academy.telemetry.end"); ok {
		moveTelemetryRuntime(task["custom_metadata"].(map[string]any), telemetry, "telemetry_runtime_at_end")
		task["telemetry_at_end"] = telemetry
	}
	addParsedMetadata(task["custom_metadata"].(map[string]any), attrs)
	return task
}

func moveTelemetryRuntime(customMetadata map[string]any, telemetry map[string]any, key string) {
	runtime, ok := telemetry["runtime"]
	if !ok {
		return
	}
	customMetadata[key] = runtime
	delete(telemetry, "runtime")
}

func addTaskResourceFields(task map[string]any, resourceAttrs map[string]any) {
	resourceMappings := map[string]string{
		"user.name":           "user",
		"host.name":           "hostname",
		"host.id":             "environment_id",
		"k8s.node.name":       "node_name",
		"service.instance.id": "environment_id",
		"server.address":      "address",
		"net.host.name":       "address",
	}
	for resourceKey, taskKey := range resourceMappings {
		if _, exists := task[taskKey]; exists {
			continue
		}
		if value := stringAttr(resourceAttrs, resourceKey); value != "" {
			task[taskKey] = cleanAcademyID(value)
		}
	}
}

func (e *flowceptExporter) campaignID(attrs map[string]any, resourceAttrs map[string]any) string {
	if e.cfg.CampaignID != "" {
		return e.cfg.CampaignID
	}
	for _, key := range []string{"flowcept.campaign_id", "campaign.id", "campaign_id"} {
		if value := stringAttr(attrs, key); value != "" {
			return value
		}
		if value := stringAttr(resourceAttrs, key); value != "" {
			return value
		}
	}
	return ""
}

func activityIDForSpan(span ptrace.Span, attrs map[string]any) string {
	switch stringAttr(attrs, "openinference.span.kind") {
	case "TOOL":
		if toolName := stringAttr(attrs, "tool.name"); toolName != "" {
			return toolName
		}
	case "AGENT":
		if agentName := stringAttr(attrs, "agent.name"); agentName != "" {
			return agentName
		}
	}
	return span.Name()
}

func eventsAsRaw(events ptrace.SpanEventSlice) []any {
	out := make([]any, 0, events.Len())
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		out = append(out, map[string]any{
			"name":       event.Name(),
			"timestamp":  unixSeconds(event.Timestamp()),
			"attributes": event.Attributes().AsRaw(),
		})
	}
	return out
}

func sourceAgentIDForSpan(openInferenceKind string, attrs map[string]any, events ptrace.SpanEventSlice) string {
	if sourceAgentID := stringAttr(attrs, "academy.message.src"); sourceAgentID != "" {
		return sourceAgentID
	}
	if openInferenceKind == "TOOL" {
		if sourceID := stringAttr(attrs, "academy.action.source_id"); sourceID != "" {
			return sourceID
		}
	}
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		if event.Name() != "academy.message.request.received" {
			continue
		}
		if sourceAgentID := stringAttr(event.Attributes().AsRaw(), "academy.message.src"); sourceAgentID != "" {
			return sourceAgentID
		}
	}
	return ""
}

func messageTagForSpan(attrs map[string]any, events ptrace.SpanEventSlice) string {
	if messageTag := stringAttr(attrs, "academy.message.tag"); messageTag != "" {
		return messageTag
	}
	for i := 0; i < events.Len(); i++ {
		if messageTag := stringAttr(events.At(i).Attributes().AsRaw(), "academy.message.tag"); messageTag != "" {
			return messageTag
		}
	}
	return ""
}

func addParsedMetadata(customMetadata map[string]any, attrs map[string]any) {
	metadata := stringAttr(attrs, "metadata")
	if metadata == "" {
		return
	}
	var parsed any
	if err := json.Unmarshal([]byte(metadata), &parsed); err == nil {
		customMetadata["openinference_metadata"] = normalizeAcademyIDs(parsed)
	}
}

func telemetryForEvent(events ptrace.SpanEventSlice, name string) (map[string]any, bool) {
	for i := events.Len() - 1; i >= 0; i-- {
		event := events.At(i)
		if event.Name() != name {
			continue
		}
		attrs := event.Attributes().AsRaw()
		telemetryJSON := stringAttr(attrs, "flowcept.telemetry")
		if telemetryJSON == "" {
			continue
		}
		var parsed map[string]any
		if err := json.Unmarshal([]byte(telemetryJSON), &parsed); err != nil {
			continue
		}
		return sanitizeMongoKeys(normalizeAcademyIDs(parsed)).(map[string]any), true
	}
	return nil, false
}

func normalizeAcademyIDs(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = normalizeAcademyIDs(item)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, normalizeAcademyIDs(item))
		}
		return out
	case string:
		return academyIDPattern.ReplaceAllString(typed, "$1")
	default:
		return value
	}
}

func sanitizeMongoKeys(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[sanitizeMongoKey(key)] = sanitizeMongoKeys(item)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, sanitizeMongoKeys(item))
		}
		return out
	default:
		return value
	}
}

func sanitizeMongoKey(key string) string {
	out := []rune(key)
	for i, char := range out {
		if char == '.' || char == '$' {
			out[i] = '_'
		}
	}
	return string(out)
}

func cleanAcademyID(value string) string {
	if value == "" {
		return ""
	}
	cleaned := academyIDPattern.ReplaceAllString(value, "$1")
	start := strings.Index(cleaned, "<")
	end := strings.LastIndex(cleaned, ">")
	if start >= 0 && end > start {
		return cleaned[start+1 : end]
	}
	return cleaned
}

func (e *flowceptExporter) publishObject(ctx context.Context, object map[string]any) error {
	payload, err := msgpack.Marshal(object)
	if err != nil {
		return err
	}
	return e.publisher.Publish(ctx, e.cfg.Channel, payload)
}

func subtypeForSpan(span ptrace.Span, attrs map[string]any) string {
	if kind := stringAttr(attrs, "openinference.span.kind"); kind != "" {
		return kind
	}
	switch span.Kind() {
	case ptrace.SpanKindProducer:
		return "otel_producer"
	case ptrace.SpanKindConsumer:
		return "otel_consumer"
	case ptrace.SpanKindClient:
		return "otel_client"
	case ptrace.SpanKindServer:
		return "otel_server"
	default:
		return "otel_span"
	}
}

func statusForSpan(span ptrace.Span) string {
	if span.Status().Code() == ptrace.StatusCodeError {
		return statusError
	}
	return statusFinished
}

func unixSeconds(t pcommon.Timestamp) float64 {
	if t == 0 {
		return 0
	}
	return float64(t.AsTime().UnixNano()) / float64(time.Second)
}

func nowUnixSeconds() float64 {
	return float64(time.Now().UnixNano()) / float64(time.Second)
}

func stringAttr(attrs map[string]any, key string) string {
	value, ok := attrs[key]
	if !ok || value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func flowceptIO(attrs map[string]any, key string) (map[string]any, bool) {
	value, ok := attrs[key]
	if !ok || value == nil {
		return nil, false
	}
	text := fmt.Sprint(value)
	if text == "" {
		return nil, false
	}
	var parsed any
	if err := json.Unmarshal([]byte(text), &parsed); err == nil {
		return map[string]any{
			"value":     parsed,
			"mime_type": stringAttr(attrs, companionMimeTypeKey(key)),
		}, true
	}
	return map[string]any{
		"value":     text,
		"mime_type": stringAttr(attrs, companionMimeTypeKey(key)),
	}, true
}

func companionMimeTypeKey(valueKey string) string {
	switch valueKey {
	case "input.value":
		return "input.mime_type"
	case "output.value":
		return "output.mime_type"
	default:
		return ""
	}
}
