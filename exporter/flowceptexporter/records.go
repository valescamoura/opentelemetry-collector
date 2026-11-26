// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package flowceptexporter // import "github.com/valescamoura/opentelemetry-collector/exporter/flowceptexporter"

import (
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// OpenInference semantic convention attribute keys used by the mapping.
const (
	attrSessionID      = "session.id"
	attrUserID         = "user.id"
	attrAgentName      = "agent.name"
	attrMetadata       = "metadata"
	attrSpanKind       = "openinference.span.kind"
	attrInputValue     = "input.value"
	attrInputMimeType  = "input.mime_type"
	attrOutputValue    = "output.value"
	attrOutputMimeType = "output.mime_type"

	mimeJSON = "application/json"

	statusFinished = "FINISHED"
	statusError    = "ERROR"

	// ruleRoot is the rule key of the root span of a trace.
	ruleRoot = "root"

	defaultInputKey  = "input"
	defaultOutputKey = "output"
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

// spanKind is the lowercase openinference.span.kind of the span.
func (r spanRecord) spanKind() string {
	return strings.ToLower(r.stringAttr(attrSpanKind))
}

// metadata decodes the OpenInference metadata attribute, a JSON object
// serialized as a string. It is nil when absent or not an object.
func (r spanRecord) metadata() map[string]any {
	raw := r.stringAttr(attrMetadata)
	if raw == "" {
		return nil
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil
	}
	return decoded
}

func toEpochSeconds(ts pcommon.Timestamp) float64 {
	return float64(ts.AsTime().UnixNano()) / float64(time.Second)
}

// ruleFor returns the rule of a span from rules keyed by "root" or span kind.
func ruleFor[T any](rules map[string]T, r spanRecord) (T, bool) {
	if r.isRoot() {
		if v, ok := rules[ruleRoot]; ok {
			return v, true
		}
	}
	v, ok := rules[r.spanKind()]
	return v, ok
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

// firstOf returns the first non-empty string attribute among the spans,
// trying the root span first.
func firstOf(spans []spanRecord, key string) string {
	for _, s := range spans {
		if s.isRoot() {
			if v := s.stringAttr(key); v != "" {
				return v
			}
		}
	}
	for _, s := range spans {
		if v := s.stringAttr(key); v != "" {
			return v
		}
	}
	return ""
}

// metadataOf returns the OpenInference metadata of a trace, from its root
// span if it has one, else from the first span that does.
func metadataOf(spans []spanRecord) map[string]any {
	for _, s := range spans {
		if s.isRoot() {
			if m := s.metadata(); m != nil {
				return m
			}
		}
	}
	for _, s := range spans {
		if m := s.metadata(); m != nil {
			return m
		}
	}
	return nil
}

// campaignOf returns the campaign_id of a trace: the session, or with
// CampaignMetadataKey the value of that metadata key.
func campaignOf(cfg *Config, spans []spanRecord, session string) string {
	if cfg.CampaignMetadataKey == "" {
		return session
	}
	for _, s := range spans {
		if v, ok := s.metadata()[cfg.CampaignMetadataKey].(string); ok && v != "" {
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

// taskIDFor builds the task_id of a span: by its TaskIDs rule when the span
// has the rule's attribute, else its span id.
func taskIDFor(cfg *Config, r spanRecord) string {
	if rule, ok := ruleFor(cfg.TaskIDs, r); ok {
		if v := r.stringAttr(rule.Attribute); v != "" {
			return rule.Prefix + v
		}
	}
	return r.spanID
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

// truncate cuts a string value to max characters, marking the cut with "…".
// Other values and max <= 0 leave the value as it is.
func truncate(value any, max int) any {
	s, ok := value.(string)
	if !ok || max <= 0 || utf8.RuneCountInString(s) <= max {
		return value
	}
	runes := []rune(s)
	return string(runes[:max]) + "…"
}

// traceInfo is what all tasks of a trace share.
type traceInfo struct {
	session    string
	workflowID string
	campaignID string
	// taskIDs maps the span ids of the trace whose task_id is not the span
	// id (TaskIDs rules), so children point to the right parent task.
	taskIDs map[string]string
}

func (t traceInfo) taskIDOf(spanID string) string {
	if id, ok := t.taskIDs[spanID]; ok {
		return id
	}
	return spanID
}

// taskRecord maps one span to a Flowcept task record.
func taskRecord(cfg *Config, r spanRecord, t traceInfo, now float64) map[string]any {
	task := map[string]any{
		"type":          "task",
		"task_id":       taskIDFor(cfg, r),
		"workflow_id":   t.workflowID,
		"activity_id":   r.name,
		"started_at":    r.startedAt,
		"ended_at":      r.endedAt,
		"utc_timestamp": now,
		"adapter_id":    cfg.AdapterID,
		"status":        statusFinished,
	}
	if r.statusCode == ptrace.StatusCodeError {
		task["status"] = statusError
	}
	if t.campaignID != "" {
		task["campaign_id"] = t.campaignID
	}
	if r.parentSpanID != "" {
		task["parent_task_id"] = t.taskIDOf(r.parentSpanID)
	}
	if subtype, ok := ruleFor(cfg.Subtypes, r); ok {
		task["subtype"] = subtype
	} else if kind := r.spanKind(); kind != "" {
		task["subtype"] = kind
	}
	if agent := r.stringAttr(attrAgentName); agent != "" {
		task["agent_id"] = agentIDFor(agent, t.session)
	}
	if user := r.stringAttr(attrUserID); user != "" {
		task["user"] = user
	}

	keys := IOKeys{Input: defaultInputKey, Output: defaultOutputKey}
	if custom, ok := ruleFor(cfg.IOKeys, r); ok {
		if custom.Input != "" {
			keys.Input = custom.Input
		}
		if custom.Output != "" {
			keys.Output = custom.Output
		}
	}
	if _, ok := r.attributes[attrInputValue]; ok {
		value := decodeValue(r.stringAttr(attrInputValue), r.stringAttr(attrInputMimeType))
		task["used"] = map[string]any{keys.Input: truncate(value, cfg.MaxText)}
	}
	if _, ok := r.attributes[attrOutputValue]; ok {
		value := decodeValue(r.stringAttr(attrOutputValue), r.stringAttr(attrOutputMimeType))
		task["generated"] = map[string]any{keys.Output: truncate(value, cfg.MaxText)}
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
	if cfg.MetadataNamespace != "" {
		if m := r.metadata(); m != nil {
			metadata[cfg.MetadataNamespace] = m
		}
	}
	task["custom_metadata"] = metadata
	return task
}

// workflowRecord builds the Flowcept workflow record of a session.
func workflowRecord(cfg *Config, spans []spanRecord, t traceInfo, now float64) map[string]any {
	wf := map[string]any{
		"type":          "workflow",
		"workflow_id":   t.workflowID,
		"name":          t.workflowID,
		"adapter_id":    cfg.AdapterID,
		"utc_timestamp": now,
	}
	if t.campaignID != "" {
		wf["campaign_id"] = t.campaignID
	}
	if cfg.WorkflowSubtype != "" {
		wf["subtype"] = cfg.WorkflowSubtype
	}
	if user := firstOf(spans, attrUserID); user != "" {
		wf["user"] = user
	}
	if cfg.MetadataNamespace != "" {
		if m := metadataOf(spans); m != nil {
			wf["custom_metadata"] = map[string]any{cfg.MetadataNamespace: m}
		}
	}
	if started := earliestStart(spans); started > 0 {
		wf["started_at"] = started
	}
	return wf
}

func earliestStart(spans []spanRecord) float64 {
	var out float64
	for _, s := range spans {
		if s.startedAt > 0 && (out == 0 || s.startedAt < out) {
			out = s.startedAt
		}
	}
	return out
}

// agentIDFor builds the Flowcept agent_id of an agent in a session. A Flowcept
// agent record describes one agent execution, so each session gets its own.
func agentIDFor(agentName, session string) string {
	if session != "" {
		return agentName + ":" + session
	}
	return agentName
}

// agentRecord builds the Flowcept agent record of an agent in a session.
func agentRecord(cfg *Config, r spanRecord, t traceInfo) map[string]any {
	agentName := r.stringAttr(attrAgentName)
	agent := map[string]any{
		"type":          "agent",
		"agent_id":      agentIDFor(agentName, t.session),
		"name":          agentName,
		"workflow_id":   t.workflowID,
		"registered_at": r.startedAt,
		"adapter_id":    cfg.AdapterID,
	}
	if t.campaignID != "" {
		agent["campaign_id"] = t.campaignID
	}
	if user := r.stringAttr(attrUserID); user != "" {
		agent["user"] = user
	}
	return agent
}
