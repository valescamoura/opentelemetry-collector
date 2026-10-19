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
}

// Validate checks the configuration.
func (cfg *Config) Validate() error {
	if cfg.Path == "" {
		return errors.New("path must be set")
	}
	if cfg.TraceTimeout <= 0 {
		return errors.New("trace_timeout must be positive")
	}
	return nil
}
