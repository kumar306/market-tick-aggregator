package config

import (
	"fmt"
	"market-adapter/constants"
	"market-adapter/logger"
	"math"
	"net/url"
	"os"
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

	if len(c.BootstrapServers) == 0 {
		return logger.LogAndWrap("Missing bootstrap servers in config file. Exiting.", nil)
	}

	seenNames := make(map[string]bool)
	seenKafkaTopics := make(map[string]bool)
	seenUrls := make(map[string]bool)

	for _, feed := range c.Feeds {

		// Each name should not be blank
		if strings.TrimSpace(feed.Name) == "" {
			return logger.LogAndWrap("cannot have blank name",
				nil)
		}

		// Each URL should not be blank, should have a proper URL
		if feed.Url == "" {
			return logger.LogAndWrap("cannot have blank url",
				nil,
				"name", feed.Name)
		}

		_, err := url.ParseRequestURI(feed.Url)
		if err != nil {
			return logger.LogAndWrap("cannot have invalid url",
				nil,
				"name", feed.Name,
				"url", feed.Url)
		}

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

		for _, stream := range feed.Streams {
			if seenKafkaTopics[stream.KafkaTopic] {
				return logger.LogAndWrap("cannot have duplicate Kafka Topic for feed",
					nil,
					"name", feed.Name,
					"kafka_topic", stream.KafkaTopic)
			}
			seenKafkaTopics[stream.KafkaTopic] = true

			validateErr := Validate(feed, stream)
			if validateErr != nil {
				return logger.LogAndWrap("internal validation error",
					validateErr,
					"name", feed.Name,
					"url", feed.Url,
					"err", validateErr)
			}
		}

		seenNames[feed.Name] = true
		seenUrls[feed.Url] = true
	}

	return nil
}

func Validate(f *constants.Feed, s *constants.Stream) error {

	if len(s.ProductIds) == 0 {
		return logger.LogAndWrap("Missing product IDs for stream", nil, "name", f.Name, "stream", s.Channel)
	}

	// default for max retries
	if s.MaxRetries <= 0 {
		logger.Log.Warn("Missing max retries or its <= 0. Reverting to default", "name", f.Name)
		s.MaxRetries = 5
	}
	if s.MaxRetries > 15 {
		return logger.LogAndWrap("cannot have more than 15 max retries for feed",
			nil,
			"name", f.Name,
			"url", f.Url,
			"max_retries", s.MaxRetries)
	}

	// default for max jitter ms
	if s.MaxJitterMillis <= 0 {
		logger.Log.Warn("Missing max jitter millis or its set to <= 0. Reverting to default", "name", f.Name)
		s.MaxJitterMillis = 1000
	}
	if s.MaxJitterMillis > 10000 {
		return logger.LogAndWrap("cannot have max jitter of more than 10 seconds for feed",
			nil,
			"name", f.Name,
			"url", f.Url,
			"max_jitter_millis", s.MaxJitterMillis)
	}

	if s.BaseDelay <= 0 {
		logger.Log.Warn("Missing base delay seconds or its set to <= 0. Reverting to default", "name", f.Name)
		s.BaseDelay = 5
	}
	if s.BaseDelay > 20 {
		return logger.LogAndWrap("cannot have base delay of more than 10 seconds for feed",
			nil,
			"name", f.Name,
			"url", f.Url,
			"base_delay", s.BaseDelay)
	}

	// default value for heartbeat interval if its 0 or not given
	if s.HearbeatInterval <= 0 {
		logger.Log.Warn("Missing heartbeat interval or its set to <= 0. Reverting to default", "name", f.Name)
		s.HearbeatInterval = 10
	}

	// default value for pong timeout = 1.5 * heartbeat interval
	if s.PongTimeout <= 0 {
		s.PongTimeout = int(math.Round(1.5 * float64(s.HearbeatInterval)))
	}

	if s.PongTimeout < int(math.Round(1.5*float64(s.HearbeatInterval))) ||
		s.PongTimeout > int(math.Round(2.5*float64(s.HearbeatInterval))) {
		return logger.LogAndWrap(
			"pong timeout should be between 1.5 to 2.5 times for heartbeat interval for feed",
			nil,
			"feed", f.Name,
			"url", f.Url,
			"heartbeat_interval", s.HearbeatInterval,
			"pong_timeout", s.PongTimeout,
			"expected_min_pong_timeout", 1.5*float64(s.HearbeatInterval),
			"expected_max_pong_timeout", 2.5*float64(s.HearbeatInterval))
	}

	logger.Log.Info("Successfully validated for stream", "name", f.Name, "stream", s.Channel)

	return nil
}
