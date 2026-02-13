package config

import (
	"fmt"
	"market-aggregator/constants"
	"os"
	"shared/logger"
	"strings"

	"gopkg.in/yaml.v3"
)

func GetConfig(cfgFilePath string) (*constants.Config, error) {

	var c constants.Config
	yamlFile, err := os.ReadFile(cfgFilePath)
	if err != nil {
		logger.Log.Error("Error when reading file from path",
			"configFilePath", cfgFilePath,
			"err", err)
		return nil, fmt.Errorf("error when reading feed config: %w", err)
	}

	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		logger.Log.Error("Error when unmarshalling YAML", "err", err)
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	// validate correct config values
	err = Validate(&c)
	if err != nil {
		logger.Log.Error("Validation Error", "err", err)
		return nil, fmt.Errorf("validation error: %w", err)
	}

	return &c, nil
}

func Validate(cfg *constants.Config) error {

	logger.Log.Info("Starting aggregator config validation")

	if cfg.WorkerCount <= 0 {
		return logger.LogAndWrap("worker_count must be > 0", nil)
	}

	kafkaCfg := cfg.KafkaConfig

	if len(kafkaCfg.BootstrapServers) == 0 {
		return logger.LogAndWrap("Bootstrap servers should not be blank", nil)
	}

	if strings.TrimSpace(kafkaCfg.TopicConfig.Upstream) == "" {
		return logger.LogAndWrap("Upstream topic should not be blank", nil)
	}

	if strings.TrimSpace(kafkaCfg.TopicConfig.Downstream) == "" {
		return logger.LogAndWrap("Downstream topic should not be blank", nil)
	}

	if strings.TrimSpace(kafkaCfg.ConsumerGroup) == "" {
		return logger.LogAndWrap("Consumer group should not be blank", nil)
	}

	backpressureCfg := kafkaCfg.BackpressureConfig

	if backpressureCfg.QueueUsageHighThreshold <= 0 ||
		backpressureCfg.QueueUsageHighThreshold > 1 {
		return logger.LogAndWrap("Backpressure queue high threshold should be in correct limit", nil)
	}

	if backpressureCfg.QueueUsageLowThreshold < 0 ||
		backpressureCfg.QueueUsageLowThreshold >= 1 ||
		backpressureCfg.QueueUsageLowThreshold > backpressureCfg.QueueUsageHighThreshold {
		return logger.LogAndWrap("Backpressure queue low threshold should be in correct limit", nil)
	}

	if backpressureCfg.CooldownTimeMillis < 0 {
		return logger.LogAndWrap("Backpressure cooldown time millis should be >=0", nil)
	}

	if backpressureCfg.ThresholdActiveMillis <= 0 {
		return logger.LogAndWrap("Backpressure threshold active millis should be >=0", nil)
	}

	windows := cfg.WindowConfig

	seen := map[string]struct{}{}

	for _, window := range windows {

		if _, exists := seen[window.Id]; exists {
			return logger.LogAndWrap("Duplicate window id found", nil, window.Id)
		}

		seen[window.Id] = struct{}{}

		if strings.TrimSpace(window.Id) == "" {
			return logger.LogAndWrap("Window ID should not be blank", nil)
		}

		if window.FlushCadencyMs <= 0 {
			return logger.LogAndWrap("Window flush cadency ms should be >0", nil)
		}

		if window.DurationMs <= 0 {
			return logger.LogAndWrap("Window duration ms should be >0", nil)
		}

		if window.FlushCadencyMs > window.DurationMs {
			return logger.LogAndWrap("Window flush cadency ms should be <= window duration ms", nil)
		}

		if window.BucketSizeMs <= 0 {
			return logger.LogAndWrap("Bucket size ms should be > 0", nil)
		}

		if window.BucketSizeMs > window.DurationMs {
			return logger.LogAndWrap("Bucket size ms should be <= window duration ms", nil)
		}
	}

	logger.Log.Info("Aggregator config validated successfully",
		"workers", cfg.WorkerCount,
		"windows", len(cfg.WindowConfig),
	)

	return nil
}
