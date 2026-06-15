// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package flowceptacademyexporter // import "go.opentelemetry.io/collector/exporter/flowceptacademyexporter"

import (
	"errors"

	"go.opentelemetry.io/collector/component"
)

const (
	defaultRedisAddr = "localhost:6379"
	defaultChannel   = "interception"
	defaultAdapterID = "otel-openinference-academy"
)

// Config defines configuration for the Flowcept Academy exporter.
type Config struct {
	RedisAddr              string `mapstructure:"redis_addr,omitempty"`
	RedisURL               string `mapstructure:"redis_url,omitempty"`
	RedisDB                int    `mapstructure:"redis_db,omitempty"`
	RedisUsername          string `mapstructure:"redis_username,omitempty"`
	RedisPassword          string `mapstructure:"redis_password,omitempty"`
	Channel                string `mapstructure:"channel,omitempty"`
	AdapterID              string `mapstructure:"adapter_id,omitempty"`
	CampaignID             string `mapstructure:"campaign_id,omitempty"`
	DisableWorkflowObjects bool   `mapstructure:"disable_workflow_objects,omitempty"`

	_ struct{}
}

var _ component.Config = (*Config)(nil)

// Validate checks if the exporter configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.RedisAddr != "" && cfg.RedisURL != "" {
		return errors.New("redis_addr and redis_url cannot both be set")
	}
	return nil
}

func (cfg *Config) normalized() Config {
	out := *cfg
	if out.RedisAddr == "" {
		out.RedisAddr = defaultRedisAddr
	}
	if out.Channel == "" {
		out.Channel = defaultChannel
	}
	if out.AdapterID == "" {
		out.AdapterID = defaultAdapterID
	}
	return out
}
