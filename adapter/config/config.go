package config

import (
	"fmt"
	"market-adapter/constants"
	"math"
	"net/url"
	"os"

	"gopkg.in/yaml.v3"
)

func GetConfig() (*constants.Config, error) {
	var c constants.Config
	yamlFile, err := os.ReadFile(constants.ConfigFile)
	if err != nil {
		return nil, fmt.Errorf("error when reading feed config: %v", err)
	}

	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		return nil, fmt.Errorf("unmarshal error: %v", err)
	}

	err = ValidateAll(&c)
	if err != nil {
		return nil, fmt.Errorf("validation error: %v", err)
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
			return fmt.Errorf("cannot have duplicate name for feed: %s", feed.Name)
		}
		if seenUrls[feed.Url] {
			return fmt.Errorf("cannot have duplicate url for feed: %s", feed.Url)
		}
		if seenKafkaTopics[feed.KafkaTopic] {
			return fmt.Errorf("cannot have duplicate kafka topic for feed: %s", feed.KafkaTopic)
		}
		seenNames[feed.Name] = true
		seenUrls[feed.Url] = true
		seenKafkaTopics[feed.KafkaTopic] = true

		validateErr := Validate(feed)
		if validateErr != nil {
			return fmt.Errorf("internal validation error: %w", validateErr)
		}
	}

	return nil
}

func Validate(f *constants.Feed) error {

	// Each URL should not be blank, should have a proper URL
	if f.Url == "" {
		return fmt.Errorf("error: url is blank for feed: %v", f.Name)
	}

	_, err := url.ParseRequestURI(f.Url)
	if err != nil {
		return fmt.Errorf("error: feed url is invalid for feed: %v", f.Name)
	}

	// Format must be of type FormatType - JSON or CSV or FIX or array of json, etc
	if f.Format != constants.FormatJson &&
		f.Format != constants.FormatCsv &&
		f.Format != constants.FormatFix {
		return fmt.Errorf("error: format is invalid for feed: %v", f.Name)
	}

	// default for max retries
	if f.MaxRetries == 0 {
		f.MaxRetries = 5
	}
	if f.MaxRetries > 15 {
		return fmt.Errorf("cannot have more than 15 max retries for feed %v", f.Name)
	}

	// default for max jitter ms
	if f.MaxJitterMillis == 0 {
		f.MaxJitterMillis = 1000
	}
	if f.MaxJitterMillis > 10000 {
		return fmt.Errorf("cannot have max jitter of more than 10 seconds for feed %v", f.Name)
	}

	if f.BaseDelay == 0 {
		f.BaseDelay = 5
	}
	if f.BaseDelay > 20 {
		return fmt.Errorf("cannot have base delay of more than 10 seconds for feed %v", f.Name)
	}

	// default value for heartbeat interval if its 0 or not given
	if f.HearbeatInterval == 0 {
		f.HearbeatInterval = 10
	}

	// default value for pong timeout = 1.5 * heartbeat interval
	if f.PongTimeout == 0 {
		f.PongTimeout = int(math.Round(1.5 * float64(f.HearbeatInterval)))
	}

	if f.PongTimeout < int(math.Round(1.5*float64(f.HearbeatInterval))) ||
		f.PongTimeout > int(math.Round(2.5*float64(f.HearbeatInterval))) {
		return fmt.Errorf("pong timeout should be between 1.5 to 2.5 times for heartbeat interval for feed: %v", f.Name)
	}

	// default for kafka batch size
	if f.KafkaBatchSize <= 1000 {
		f.KafkaBatchSize = 10000
	}

	return nil
}
