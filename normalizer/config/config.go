package config

import (
	"fmt"
	"market-normalizer/constants"
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

func Validate(c *constants.Config) error {
	// TODO: config validation
	// verify topics is a valid array of strings. if no topics, return err
	// verify bootstrap servers is valid array of strings.
	// verify max poll records is present. and a number. if a negative number or not present, default is 500
	// verify similarly for commit millis, default to 5000

	var kafkaConfig *constants.KafkaConfig = c.KafkaConfig

	if len(kafkaConfig.Brokers) == 0 {
		return logger.LogAndWrap("Normalizer kafka brokers length = 0. Exiting.", nil)
	}

	if len(kafkaConfig.Topics) == 0 {
		return logger.LogAndWrap("Normalizer kafka consume topics length = 0. Exiting.", nil)
	}

	if strings.TrimSpace(kafkaConfig.ConsumerGroup) == "" {
		logger.Log.Warn("Normalizer kafka consumer is blank. Setting to default [normalizer-group-1]")
		kafkaConfig.ConsumerGroup = constants.DefaultConsumerGroupName
	}

	// under 100 is too less to fetch at once
	if kafkaConfig.MaxBufferRecords <= 100 {
		logger.Log.Warn("Normalizer kafka max poll records missing or <= 10. Setting to default [100]")
		kafkaConfig.MaxBufferRecords = 100
	}

	if kafkaConfig.CommitOffsetIntervalMillis < 2000 {
		logger.Log.Warn("Normalizer kafka commit offset interval millis missing or < 2 seconds. Setting to default [2 seconds]")
		kafkaConfig.CommitOffsetIntervalMillis = 2000
	}

	return nil
}
