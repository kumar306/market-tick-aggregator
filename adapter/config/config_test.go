package config_test

import (
	"bytes"
	"encoding/json"
	"market-adapter/config"
	"market-adapter/constants"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

// i'll create a table driven test to test different config.yaml
// i. valid config
// ii. missing feed name
// iii. missing feed url
// iv. missing streams
// v. invalid format in feed
// vi. invalid format in stream
// vii. incorrect information in stream/feed
// viii. missing bootstrap servers or product ids
func Test_GetConfig(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name          string
		yamlContent   string
		expectedError bool
	}{
		{
			name: "valid config",
			yamlContent: `
bootstrap_servers:
  - localhost:9092
feeds:
  - name: binance
    url: wss://stream.binance.com:9443/ws
    streams:
      - name: binance
        channel: aggTrade
        productIds:
          - btcusdt
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: binance_raw_aggtrades
        ringBufferSize: 1024
      - name: binance
        channel: depth
        productIds:
          - btcusdt
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: binance_raw_depth
        ringBufferSize: 1024
  - name: coinbase
    url: wss://ws-feed.exchange.coinbase.com
    streams:
      - channel: ticker
        productIds:
          - BTC-USD
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: coinbase_raw_ticker
        ringBufferSize: 1024
      - channel: level2
        productIds:
          - BTC-USD
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: coinbase_raw_depth
        ringBufferSize: 1024
  - name: kraken
    url: wss://ws.kraken.com/v2
    streams: 
      - name: kraken
        channel: ticker
        productIds:
          - BTC/USD
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: kraken_raw_ticker
        ringBufferSize: 1024
      - name: kraken
        channel: book
        productIds:
          - BTC/USD
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: kraken_raw_depth
        ringBufferSize: 1024`,
			expectedError: false,
		},
		{
			name: "missing feed name",
			yamlContent: `
bootstrap_servers:
  - localhost:9092
feeds:
  - url: wss://stream.binance.com:9443/ws
    streams:
      - name: binance
        channel: aggTrade
        productIds:
          - btcusdt
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: binance_raw_aggtrades
        ringBufferSize: 1024`,
			expectedError: true,
		},
		{
			name: "missing feed url",
			yamlContent: `
bootstrap_servers:
  - localhost:9092
feeds:
  - name: binance
    streams:
      - name: binance
        channel: aggTrade
        productIds:
          - btcusdt
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: binance_raw_aggtrades
        ringBufferSize: 1024`,
			expectedError: true,
		},
		{
			name: "invalid feed url",
			yamlContent: `
bootstrap_servers:
  - localhost:9092
feeds:
  - name: binance
	url: abcd12345
    streams:
      - name: binance
        channel: aggTrade
        productIds:
          - btcusdt
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: binance_raw_aggtrades
        ringBufferSize: 1024`,
			expectedError: true,
		},
		{
			name: "missing streams array",
			yamlContent: `
bootstrap_servers:
  - localhost:9092
feeds:
  - name: binance
	url: wss://stream.binance.com:9443/ws
	`,
			expectedError: true,
		},
		{
			name: "invalid stream value",
			yamlContent: `
bootstrap_servers:
  - localhost:9092
feeds:
  - name: binance
    url: wss://stream.binance.com:9443/ws
    streams:
      - name: binance
        channel: aggTrade
        productIds:
          - btcusdt
        maxRetries: a
        heartBeatInterval: b
        pongTimeout: c
        kafkaTopic: binance_raw_aggtrades
        ringBufferSize: 1024
  - name: coinbase
    url: wss://ws-feed.exchange.coinbase.com
    streams:
      - channel: ticker
        productIs:
          - BTC-USD
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: coinbase_raw_ticker
        ringBufferSize: 1024
      - channel: level2
        productIds:
          - BTC-USD
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: coinbase_raw_depth
        ringBufferSize: 1024
    `,
			expectedError: true,
		},
		{
			name: "invalid yaml",
			yamlContent: `
bootstrap_servers:
  - localhost:9092
feeds:
  - name: binance
    url: wss://stream.binance.com:9443/ws
    streams:
      - name: binance
        channel "aggTrade"
        productIds:
          - btcusdt
        maxRetries: a
        heartBeatInterval: b
        pongTimeout: c
        kafkaTopic: binance_raw_aggtrades
        ringBufferSize: 1024
  - name: coinbase
    url: wss://ws-feed.exchange.coinbase.com
    streams:
      - channel: ticker
        productIs:
          - BTC-USD
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: coinbase_raw_ticker
        ringBufferSize: 1024
      - channel: level2
        productIds:
          - BTC-USD
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: coinbase_raw_depth
        ringBufferSize: 1024
    `,
			expectedError: true,
		}, {

			name: "invalid ring buffer size",
			yamlContent: `
bootstrap_servers:
  - localhost:9092
feeds:
  - name: binance
    url: wss://stream.binance.com:9443/ws
    streams:
      - name: binance
        channel: aggTrade
        productIds:
          - btcusdt
        maxRetries: a
        heartBeatInterval: b
        pongTimeout: c
        kafkaTopic: binance_raw_aggtrades
        ringBufferSize: 1024
  - name: coinbase
    url: wss://ws-feed.exchange.coinbase.com
    streams:
      - channel: ticker
        productIs:
          - BTC-USD
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: coinbase_raw_ticker
        ringBufferSize: 1024
      - channel: level2
        productIds:
          - BTC-USD
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: coinbase_raw_depth
        ringBufferSize: 1040
    `,
			expectedError: true,
		},
		{
			name: "missing/invalid product Ids",
			yamlContent: `
bootstrap_servers:
  - localhost:9092
feeds:
  - name: binance
    url: wss://stream.binance.com:9443/ws
    streams:
      - name: binance
        channel: aggTrade
        productIds:
        maxRetries: a
        heartBeatInterval: b
        pongTimeout: c
        kafkaTopic: binance_raw_aggtrades
        ringBufferSize: 1024
  - name: coinbase
    url: wss://ws-feed.exchange.coinbase.com
    streams:
      - channel: ticker
        productIs: BTC-USD
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: coinbase_raw_ticker
        ringBufferSize: 1024
      - channel: level2
        productIds:
          - BTC-USD
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: coinbase_raw_depth
        ringBufferSize: 1040
    `,
			expectedError: true,
		},
		{
			name: "missing bootstrap servers",
			yamlContent: `
feeds:
  - name: binance
  	url: wss://stream.binance.com:9443/ws
    streams:
      - name: binance
        channel: aggTrade
        productIds:
          - btcusdt
        maxRetries: 5
        heartBeatInterval: 15
        pongTimeout: 25
        kafkaTopic: binance_raw_aggtrades
        ringBufferSize: 1024`,
			expectedError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfgFilePath := filepath.Join(tempDir, "config_test.yaml")
			err := os.WriteFile(cfgFilePath, []byte(tc.yamlContent), 0644)
			if err != nil {
				t.Fatalf("Could not write yaml to temp file for tc: %v. Error: %v", tc.name, err)
			}

			cfg, err := config.GetConfig(cfgFilePath)

			if tc.expectedError && err == nil {
				t.Fatalf("Expected error but got nil for test %v", tc.name)
			}

			if !tc.expectedError && err != nil {
				t.Fatalf("Expected no error but got: %v", err)
			}

			if !tc.expectedError && cfg == nil {
				t.Fatalf("Expected non-nil config but got nil")
			}
		})
	}
}

// to test that no extra fields are added in the config and that schema matches
func Test_ConfigSchema(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("Unable to get working directory: %v", err)
	}

	cfgFilePath := filepath.Join(root, "./config.yaml")
	yamlData, err := os.ReadFile(cfgFilePath)
	if err != nil {
		t.Fatalf("Unable to read yaml file: %v", err)
	}

	// convert the yaml to json and specify the strict decoder option for json to throw err for field schema mismatch
	var temp interface{}
	if err = yaml.Unmarshal(yamlData, &temp); err != nil {
		t.Fatalf("Unable to parse YAML file: %v", err)
	}

	jsonData, _ := json.Marshal(temp)

	var cfg constants.Config
	decoder := json.NewDecoder(bytes.NewReader(jsonData))
	decoder.DisallowUnknownFields()

	if err = decoder.Decode(&cfg); err != nil {
		t.Fatalf("Schema validation mismatch. Test failed. Error: %v", err)
	}
}
