// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package flowceptexporter // import "github.com/valescamoura/opentelemetry-collector/exporter/flowceptexporter"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	typeStr          = "flowcept"
	tracesStability  = component.StabilityLevelDevelopment
	defaultPath      = "flowcept_otel_buffer.jsonl"
	defaultAdapterID = "otel_openinference"
	defaultTimeout   = 5 * time.Minute
)

var componentType = component.MustNewType(typeStr)

// NewFactory creates a factory for the Flowcept exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		componentType,
		createDefaultConfig,
		exporter.WithTraces(createTraces, tracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Path:             defaultPath,
		WorkflowIDPrefix: "session:",
		AdapterID:        defaultAdapterID,
		TraceTimeout:     defaultTimeout,
	}
}

func createTraces(ctx context.Context, set exporter.Settings, config component.Config) (exporter.Traces, error) {
	cfg := config.(*Config)
	exp := newFlowceptExporter(cfg, set.Logger)
	return exporterhelper.NewTraces(ctx, set, config,
		exp.pushTraces,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: 0}),
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
	)
}
