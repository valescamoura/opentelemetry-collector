// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package flowceptacademyexporter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.Equal(t, defaultRedisAddr, cfg.RedisAddr)
	require.Equal(t, defaultChannel, cfg.Channel)
	require.Equal(t, defaultAdapterID, cfg.AdapterID)
}

func TestConfigValidateRejectsAddrAndURL(t *testing.T) {
	cfg := &Config{
		RedisAddr: "localhost:6379",
		RedisURL:  "redis://localhost:6379",
	}
	require.Error(t, cfg.Validate())
}
