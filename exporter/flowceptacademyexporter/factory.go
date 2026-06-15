// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package flowceptacademyexporter // import "go.opentelemetry.io/collector/exporter/flowceptacademyexporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

var componentType = component.MustNewType("flowceptacademy")

// NewFactory creates a factory for the Flowcept exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		componentType,
		createDefaultConfig,
		exporter.WithTraces(createTraces, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		RedisAddr: defaultRedisAddr,
		Channel:   defaultChannel,
		AdapterID: defaultAdapterID,
	}
}

func createTraces(ctx context.Context, set exporter.Settings, config component.Config) (exporter.Traces, error) {
	cfg := config.(*Config).normalized()
	publisher, err := newRedisPublisher(cfg)
	if err != nil {
		return nil, err
	}
	exp := newFlowceptExporter(cfg, publisher)
	return exporterhelper.NewTraces(
		ctx,
		set,
		config,
		exp.pushTraces,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithShutdown(exp.shutdown),
	)
}
