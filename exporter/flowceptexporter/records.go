// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package flowceptexporter // import "github.com/valescamoura/opentelemetry-collector/exporter/flowceptexporter"

import (
	"encoding/json"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// OpenInference semantic convention attribute keys used by the mapping.
const (
	attrSessionID      = "session.id"
	attrUserID         = "user.id"
	attrAgentName      = "agent.name"
	attrSpanKind       = "openinference.span.kind"
	attrInputValue     = "input.value"
	attrInputMimeType  = "input.mime_type"
	attrOutputValue    = "output.value"
	attrOutputMimeType = "output.mime_type"

	mimeJSON = "application/json"

	statusFinished = "FINISHED"
	statusError    = "ERROR"
)

// spanRecord is a copy of the span data the exporter needs. Spans are copied
// out of pdata because they are held in memory beyond the push call.
type spanRecord struct {
	traceID       string
	spanID        string
	parentSpanID  string
	name          string
	kind          string
	startedAt     float64
	endedAt       float64
	statusCode    ptrace.StatusCode
	statusMessage string
	attributes    map[string]any
	resource      map[string]any
	scope         string
}

func newSpanRecord(span ptrace.Span, resource pcommon.Resource, scope pcommon.InstrumentationScope) spanRecord {
	rec := spanRecord{
		traceID:       span.TraceID().String(),
		spanID:        span.SpanID().String(),
		name:          span.Name(),
		kind:          span.Kind().String(),
		startedAt:     toEpochSeconds(span.StartTimestamp()),
		endedAt:       toEpochSeconds(span.EndTimestamp()),
		statusCode:    span.Status().Code(),
		statusMessage: span.Status().Message(),
		attributes:    span.Attributes().AsRaw(),
		resource:      resource.Attributes().AsRaw(),
		scope:         scope.Name(),
	}
	if !span.ParentSpanID().IsEmpty() {
		rec.parentSpanID = span.ParentSpanID().String()
	}
	return rec
}

func (r spanRecord) isRoot() bool {
	return r.parentSpanID == ""
}

func (r spanRecord) stringAttr(key string) string {
	if v, ok := r.attributes[key].(string); ok {
		return v
	}
	return ""
}

func toEpochSeconds(ts pcommon.Timestamp) float64 {
	return float64(ts.AsTime().UnixNano()) / float64(time.Second)
}

// sessionOf returns the first session.id found among the spans of a trace.
// OpenInference instrumentations set it on agent/chain spans, not always on
// the nested LLM and tool spans, so the whole trace inherits it.
func sessionOf(spans []spanRecord) string {
	for _, s := range spans {
		if v := s.stringAttr(attrSessionID); v != "" {
			return v
		}
	}
	return ""
}

// workflowIDFor builds the Flowcept workflow_id for a trace. Traces of the
// same session share one workflow; traces without a session get their own.
func workflowIDFor(prefix, session, traceID string) string {
	if session != "" {
		return prefix + session
	}
	return traceID
}

// decodeValue parses JSON payloads so they are stored as objects instead of
// escaped strings. Non-JSON values are kept as they are.
func decodeValue(value, mimeType string) any {
	if mimeType != mimeJSON {
		return value
	}
	var decoded any
	if err := json.Unmarshal([]byte(value), &decoded); err != nil {
		return value
	}
	return decoded
}

// taskRecord maps one span to a Flowcept task record.
func taskRecord(r spanRecord, workflowID, campaignID, adapterID string, now float64) map[string]any {
	task := map[string]any{
		"type":          "task",
		"task_id":       r.spanID,
		"workflow_id":   workflowID,
		"activity_id":   r.name,
		"started_at":    r.startedAt,
		"ended_at":      r.endedAt,
		"utc_timestamp": now,
		"adapter_id":    adapterID,
		"status":        statusFinished,
	}
	if r.statusCode == ptrace.StatusCodeError {
		task["status"] = statusError
	}
	if campaignID != "" {
		task["campaign_id"] = campaignID
	}
	if r.parentSpanID != "" {
		task["parent_task_id"] = r.parentSpanID
	}
	if kind := r.stringAttr(attrSpanKind); kind != "" {
		task["subtype"] = strings.ToLower(kind)
	}
	if agent := r.stringAttr(attrAgentName); agent != "" {
		task["agent_id"] = agent
	}
	if user := r.stringAttr(attrUserID); user != "" {
		task["user"] = user
	}
	if _, ok := r.attributes[attrInputValue]; ok {
		task["used"] = map[string]any{"input": decodeValue(r.stringAttr(attrInputValue), r.stringAttr(attrInputMimeType))}
	}
	if _, ok := r.attributes[attrOutputValue]; ok {
		task["generated"] = map[string]any{"output": decodeValue(r.stringAttr(attrOutputValue), r.stringAttr(attrOutputMimeType))}
	}

	attributes := make(map[string]any, len(r.attributes))
	for k, v := range r.attributes {
		if k == attrInputValue || k == attrOutputValue {
			continue
		}
		attributes[k] = v
	}
	metadata := map[string]any{
		"trace_id":   r.traceID,
		"span_id":    r.spanID,
		"span_kind":  r.kind,
		"attributes": attributes,
		"resource":   r.resource,
		"scope":      r.scope,
	}
	if r.statusMessage != "" {
		metadata["status_message"] = r.statusMessage
	}
	task["custom_metadata"] = metadata
	return task
}

// workflowRecord builds the Flowcept workflow record of a session.
func workflowRecord(workflowID, campaignID, adapterID string, now float64) map[string]any {
	return map[string]any{
		"type":          "workflow",
		"workflow_id":   workflowID,
		"campaign_id":   campaignID,
		"name":          workflowID,
		"adapter_id":    adapterID,
		"utc_timestamp": now,
	}
}
