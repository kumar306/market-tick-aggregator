package config

import (
	"os"
	"path/filepath"
	"testing"
)

func validConfig() *Config {
	return &Config{
		BatcherConfig: &BatcherConfig{
			TickBatchConfig: &PipelineBatcherConfig{
				BatchSize:  10,
				IntervalMs: 1000,
			},
			BookBatchConfig: &PipelineBatcherConfig{
				BatchSize:  10,
				IntervalMs: 1000,
			},
		},
		KafkaConfig: &KafkaConfig{
			BootstrapServers: []string{"localhost:9092"},
			TopicConfig: &TopicConfig{
				Tick: "aggregated.ticks",
				Book: "aggregated.book",
			},
			ConsumerGroup: "persistence-group",
			BackpressureConfig: &BackpressureConfig{
				QueueUsageHighThreshold: 0.8,
				QueueUsageLowThreshold:  0.3,
				ConfirmSeconds:          3,
				PollIntervalMs:          100,
			},
		},
	}
}

func TestValidateSuccess(t *testing.T) {
	if err := Validate(validConfig()); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

func TestValidateRejectsBadConfig(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{
			name: "tick batch size too low",
			mutate: func(cfg *Config) {
				cfg.BatcherConfig.TickBatchConfig.BatchSize = 9
			},
		},
		{
			name: "book interval too low",
			mutate: func(cfg *Config) {
				cfg.BatcherConfig.BookBatchConfig.IntervalMs = 999
			},
		},
		{
			name: "empty bootstrap servers",
			mutate: func(cfg *Config) {
				cfg.KafkaConfig.BootstrapServers = nil
			},
		},
		{
			name: "blank consumer group",
			mutate: func(cfg *Config) {
				cfg.KafkaConfig.ConsumerGroup = "   "
			},
		},
		{
			name: "high threshold out of bounds",
			mutate: func(cfg *Config) {
				cfg.KafkaConfig.BackpressureConfig.QueueUsageHighThreshold = 1.5
			},
		},
		{
			name: "low threshold above high threshold",
			mutate: func(cfg *Config) {
				cfg.KafkaConfig.BackpressureConfig.QueueUsageLowThreshold = 0.9
				cfg.KafkaConfig.BackpressureConfig.QueueUsageHighThreshold = 0.8
			},
		},
		{
			name: "confirm seconds <= 0",
			mutate: func(cfg *Config) {
				cfg.KafkaConfig.BackpressureConfig.ConfirmSeconds = 0
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(cfg)
			if err := Validate(cfg); err == nil {
				t.Fatalf("Validate() error = nil, want non-nil")
			}
		})
	}
}

func TestGetConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
batcher:
  aggregated_ticks:
    batch_size: 10
    interval_ms: 1000
  orderbook_flushes:
    batch_size: 10
    interval_ms: 1000
kafka:
  bootstrap_servers:
    - localhost:9092
  topics:
    tick: aggregated_ticks
    book: aggregated_book
  consumer_group: persistence-group
  max_buffer_records: 1000
  backpressure:
    queue_usage_high_threshold: 0.8
    queue_usage_low_threshold: 0.3
    confirm_seconds: 3
    poll_interval_ms: 100
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := GetConfig(path)
	if err != nil {
		t.Fatalf("GetConfig() error = %v, want nil", err)
	}

	if cfg.KafkaConfig.ConsumerGroup != "persistence-group" {
		t.Fatalf("consumer group = %q, want %q", cfg.KafkaConfig.ConsumerGroup, "persistence-group")
	}
}

func TestLoadPostgresConfig(t *testing.T) {
	t.Setenv("POSTGRES_USER", "postgres")
	t.Setenv("POSTGRES_PASSWORD", "secret")
	t.Setenv("POSTGRES_DB", "market")
	t.Setenv("POSTGRES_HOST", "")
	t.Setenv("POSTGRES_PORT", "")
	t.Setenv("POSTGRES_MAX_CONNS", "")

	cfg, err := LoadPostgresConfig()
	if err != nil {
		t.Fatalf("LoadPostgresConfig() error = %v, want nil", err)
	}

	if cfg.Host != "localhost" {
		t.Fatalf("Host = %q, want localhost", cfg.Host)
	}
	if cfg.Port != 5432 {
		t.Fatalf("Port = %d, want 5432", cfg.Port)
	}
	if cfg.MaxConns != 10 {
		t.Fatalf("MaxConns = %d, want 10", cfg.MaxConns)
	}
}

func TestLoadPostgresConfigMissingRequiredEnv(t *testing.T) {
	t.Setenv("POSTGRES_USER", "")
	t.Setenv("POSTGRES_PASSWORD", "secret")
	t.Setenv("POSTGRES_DB", "market")

	_, err := LoadPostgresConfig()
	if err == nil {
		t.Fatalf("LoadPostgresConfig() error = nil, want non-nil")
	}
}
