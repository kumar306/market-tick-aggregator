package config

import (
	"fmt"
	"market-adapter/constants"
	"market-adapter/logger"
	"math"
	"net/url"
	"os"

	"gopkg.in/yaml.v3"
)

func GetConfig() (*constants.Config, error) {

	var c constants.Config
	yamlFile, err := os.ReadFile(constants.ConfigFile)
	if err != nil {
		logger.Log.Error("Error when reading file from path",
			"configFilePath", constants.ConfigFile,
			"err", err)
		return nil, fmt.Errorf("error when reading feed config: %w", err)
	}

	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		logger.Log.Error("Error when unmarshalling YAML", "err", err)
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	err = ValidateAll(&c)
	if err != nil {
		logger.Log.Error("Validation Error", "err", err)
		return nil, fmt.Errorf("validation error: %w", err)
	}

	return &c, nil
}

func ValidateAll(c *constants.Config) error {
	// Each feed must have distinct URL, distinct name, distinct Kafka topic.
	// No duplicates across feeds
	// Feed validation done
	// default values if not given in yaml config
	seenNames := make(map[string]bool)
	seenKafkaTopics := make(map[string]bool)
	seenUrls := make(map[string]bool)

	for _, feed := range c.Feeds {
		if seenNames[feed.Name] {
			return logger.LogAndWrap("cannot have duplicate name for feed",
				nil,
				"name", feed.Name)
		}
		if seenUrls[feed.Url] {
			return logger.LogAndWrap("cannot have duplicate url for feed",
				nil,
				"name", feed.Name,
				"url", feed.Url)
		}
		if seenKafkaTopics[feed.KafkaTopic] {
			return logger.LogAndWrap("cannot have duplicate Kafka Topic for feed",
				nil,
				"name", feed.Name,
				"kafka_topic", feed.KafkaTopic)
		}
		seenNames[feed.Name] = true
		seenUrls[feed.Url] = true
		seenKafkaTopics[feed.KafkaTopic] = true

		validateErr := Validate(feed)
		if validateErr != nil {
			return logger.LogAndWrap("internal validation error",
				validateErr,
				"name", feed.Name,
				"url", feed.Url,
				"err", validateErr)
		}
	}

	return nil
}

func Validate(f *constants.Feed) error {

	// Each URL should not be blank, should have a proper URL
	if f.Url == "" {
		return logger.LogAndWrap("cannot have blank url",
			nil,
			"name", f.Name)
	}

	_, err := url.ParseRequestURI(f.Url)
	if err != nil {
		return logger.LogAndWrap("cannot have invalid url",
			nil,
			"name", f.Name,
			"url", f.Url)
	}

	// Format must be of type FormatType - JSON or CSV or FIX or array of json, etc
	if f.Format != constants.FormatJson &&
		f.Format != constants.FormatCsv &&
		f.Format != constants.FormatFix {
		return logger.LogAndWrap("cannot have invalid format. expected json/csv/fix",
			nil,
			"name", f.Name,
			"url", f.Url,
			"format", f.Format)
	}

	// default for max retries
	if f.MaxRetries == 0 {
		logger.Log.Warn("Missing max retries or its set to 0. Reverting to default", "name", f.Name)
		f.MaxRetries = 5
	}
	if f.MaxRetries > 15 {
		return logger.LogAndWrap("cannot have more than 15 max retries for feed",
			nil,
			"name", f.Name,
			"url", f.Url,
			"max_retries", f.MaxRetries)
	}

	// default for max jitter ms
	if f.MaxJitterMillis == 0 {
		logger.Log.Warn("Missing max jitter millis or its set to 0. Reverting to default", "name", f.Name)
		f.MaxJitterMillis = 1000
	}
	if f.MaxJitterMillis > 10000 {
		return logger.LogAndWrap("cannot have max jitter of more than 10 seconds for feed",
			nil,
			"name", f.Name,
			"url", f.Url,
			"max_jitter_millis", f.MaxJitterMillis)
	}

	if f.BaseDelay == 0 {
		logger.Log.Warn("Missing base delay seconds or its set to 0. Reverting to default", "name", f.Name)
		f.BaseDelay = 5
	}
	if f.BaseDelay > 20 {
		return logger.LogAndWrap("cannot have base delay of more than 10 seconds for feed",
			nil,
			"name", f.Name,
			"url", f.Url,
			"base_delay", f.BaseDelay)
	}

	// default value for heartbeat interval if its 0 or not given
	if f.HearbeatInterval == 0 {
		logger.Log.Warn("Missing heartbeat interval or its set to 0. Reverting to default", "name", f.Name)
		f.HearbeatInterval = 10
	}

	// default value for pong timeout = 1.5 * heartbeat interval
	if f.PongTimeout == 0 {
		f.PongTimeout = int(math.Round(1.5 * float64(f.HearbeatInterval)))
	}

	if f.PongTimeout < int(math.Round(1.5*float64(f.HearbeatInterval))) ||
		f.PongTimeout > int(math.Round(2.5*float64(f.HearbeatInterval))) {
		return logger.LogAndWrap(
			"pong timeout should be between 1.5 to 2.5 times for heartbeat interval for feed",
			nil,
			"feed", f.Name,
			"url", f.Url,
			"heartbeat_interval", f.HearbeatInterval,
			"pong_timeout", f.PongTimeout,
			"expected_min_pong_timeout", 1.5*float64(f.HearbeatInterval),
			"expected_max_pong_timeout", 2.5*float64(f.HearbeatInterval))
	}

	// default for kafka batch size
	if f.KafkaBatchSize <= 1000 {
		f.KafkaBatchSize = 10000
	}

	logger.Log.Info("Successfully validated for feed", "name", f.Name)

	return nil
}
