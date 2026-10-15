package config

import (
	"market-aggregator/constants"
	"testing"

	"github.com/stretchr/testify/require"
)

// the files that actually get deployed (docker.config.yaml goes into the ConfigMap) must parse and validate
func TestShippedConfigs_LoadInAtLeastOnceMode(t *testing.T) {
	for _, path := range []string{"config.yaml", "docker.config.yaml"} {
		cfg, err := GetConfig(path)
		require.NoError(t, err, path)
		require.Equal(t, constants.DeliveryAtLeastOnce, cfg.KafkaConfig.DeliveryMode, path)
		require.Equal(t, 2000, cfg.KafkaConfig.CheckpointIntervalMillis, path)
	}
}

func TestValidate_RejectsAnUnknownDeliveryMode(t *testing.T) {
	cfg, err := GetConfig("config.yaml")
	require.NoError(t, err)
	cfg.KafkaConfig.DeliveryMode = "exactly_once_please"
	require.Error(t, Validate(cfg))
}

func TestValidate_DefaultsWhenOmitted(t *testing.T) {
	cfg, err := GetConfig("config.yaml")
	require.NoError(t, err)
	cfg.KafkaConfig.DeliveryMode = ""
	cfg.KafkaConfig.CheckpointIntervalMillis = 0
	require.NoError(t, Validate(cfg))
	require.Equal(t, constants.DeliveryTransactional, cfg.KafkaConfig.DeliveryMode)
	require.Equal(t, 2000, cfg.KafkaConfig.CheckpointIntervalMillis)
}
