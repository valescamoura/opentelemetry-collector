// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package flowceptexporter // import "github.com/valescamoura/opentelemetry-collector/exporter/flowceptexporter"

import (
	"errors"
	"time"
)

// Config defines configuration for the Flowcept exporter.
//
// The exporter converts OpenInference spans into Flowcept task and workflow
// records and appends them, one JSON object per line, to a JSONL file that
// follows the Flowcept offline buffer format (readable with
// Flowcept.read_buffer_file).
//
// Rules keyed by span (Subtypes, TaskIDs, IOKeys) use the lowercase
// openinference.span.kind of the span ("agent", "tool", "llm", ...) or the
// special key "root" for the root span of a trace. "root" wins over the kind.
type Config struct {
	// Path is the JSONL file the records are appended to.
	Path string `mapstructure:"path"`

	// WorkflowIDPrefix is prepended to session.id to build the Flowcept
	// workflow_id that groups all traces of one session.
	WorkflowIDPrefix string `mapstructure:"workflow_id_prefix"`

	// AdapterID is written to the adapter_id field of every record.
	AdapterID string `mapstructure:"adapter_id"`

	// TraceTimeout is how long spans of a trace are held waiting for the
	// trace root span before they are written anyway.
	TraceTimeout time.Duration `mapstructure:"trace_timeout"`

	// CampaignMetadataKey, when set, takes the campaign_id from this key of
	// the OpenInference metadata attribute (a JSON object, set for example
	// with openinference's using_attributes(metadata=...)) instead of from
	// session.id. A trace without that key gets no campaign.
	CampaignMetadataKey string `mapstructure:"campaign_metadata_key"`

	// MetadataNamespace, when set, copies the OpenInference metadata object
	// of a span into custom_metadata.<namespace> of its task (and of the
	// session workflow record).
	MetadataNamespace string `mapstructure:"metadata_namespace"`

	// WorkflowSubtype is written to the subtype field of the session
	// workflow record.
	WorkflowSubtype string `mapstructure:"workflow_subtype"`

	// Subtypes maps a span (kind or "root") to the subtype of its task.
	// Spans without a rule get their lowercase span kind.
	Subtypes map[string]string `mapstructure:"subtypes"`

	// TaskIDs builds the task_id of a span (kind or "root") from one of its
	// attributes instead of the span id. Children keep pointing to it.
	TaskIDs map[string]TaskIDRule `mapstructure:"task_ids"`

	// IOKeys renames the keys of used/generated for a span (kind or "root").
	// The defaults are "input" and "output".
	IOKeys map[string]IOKeys `mapstructure:"io_keys"`

	// MaxText truncates string values of used/generated to this many
	// characters (0 keeps them whole).
	MaxText int `mapstructure:"max_text"`
}

// TaskIDRule builds a task_id as Prefix + the value of Attribute. A span
// without the attribute keeps its span id.
type TaskIDRule struct {
	Attribute string `mapstructure:"attribute"`
	Prefix    string `mapstructure:"prefix"`
}

// IOKeys are the keys of used (Input) and generated (Output).
type IOKeys struct {
	Input  string `mapstructure:"input"`
	Output string `mapstructure:"output"`
}

// Validate checks the configuration.
func (cfg *Config) Validate() error {
	if cfg.Path == "" {
		return errors.New("path must be set")
	}
	if cfg.TraceTimeout <= 0 {
		return errors.New("trace_timeout must be positive")
	}
	if cfg.MaxText < 0 {
		return errors.New("max_text must not be negative")
	}
	for key, rule := range cfg.TaskIDs {
		if rule.Attribute == "" {
			return errors.New("task_ids." + key + ".attribute must be set")
		}
	}
	return nil
}
