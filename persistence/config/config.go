package config

import (
	"fmt"
	"os"
	"shared/logger"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

const (
	ConfigFile            string = "./config/config.yaml"
	EnvFile               string = "../.env"
	AggregatedTicksTopic  string = "aggregated.ticks"
	OrderbookFlushesTopic string = "aggregated.book"
	DlqTopic              string = "persistence.dlq"
	TickPipelineName      string = "tickPipeline"
	BookPipelineName      string = "bookPipeline"
)

type Config struct {
	BatcherConfig *BatcherConfig `yaml:"batcher"`
	KafkaConfig   *KafkaConfig   `yaml:"kafka"`
}

type BatcherConfig struct {
	TickBatchConfig *PipelineBatcherConfig `yaml:"aggregated_ticks"`
	BookBatchConfig *PipelineBatcherConfig `yaml:"orderbook_flushes"`
}

type PipelineBatcherConfig struct {
	BatchSize  int `yaml:"batch_size"`
	IntervalMs int `yaml:"interval_ms"`
}

type PostgresConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	MaxConns int
}

type KafkaConfig struct {
	BootstrapServers   []string            `yaml:"bootstrap_servers"`
	TopicConfig        *TopicConfig        `yaml:"topics"`
	ConsumerGroup      string              `yaml:"consumer_group"`
	BackpressureConfig *BackpressureConfig `yaml:"backpressure"`
	MaxBufferRecords   int                 `yaml:"max_buffer_records"`
}

type TopicConfig struct {
	Tick string `yaml:"tick"`
	Book string `yaml:"book"`
}

type BackpressureConfig struct {
	QueueUsageHighThreshold float64 `yaml:"queue_usage_high_threshold"`
	QueueUsageLowThreshold  float64 `yaml:"queue_usage_low_threshold"`
	ConfirmSeconds          int64   `yaml:"confirm_seconds"`
	PollIntervalMs          int64   `yaml:"poll_interval_ms"`
}

type DLQMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Payload   []byte
	ErrorMsg  string
	Timestamp time.Time
}

func GetConfig(cfgFilePath string) (*Config, error) {

	loadErr := godotenv.Load(EnvFile)
	if loadErr != nil {
		return nil, logger.LogAndWrap("Error loading .env file", loadErr)
	}

	var c Config
	yamlFile, err := os.ReadFile(cfgFilePath)
	if err != nil {
		logger.Log.Error("Error when reading file from path",
			"configFilePath", cfgFilePath,
			"err", err)
		return nil, fmt.Errorf("error when reading persistence config: %w", err)
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

func Validate(cfg *Config) error {

	logger.Log.Info("Starting persistence config validation")

	batcherCfg := cfg.BatcherConfig
	if batcherCfg.TickBatchConfig.BatchSize < 10 {
		return logger.LogAndWrap("Tick batch config size is too low", nil)
	}

	if batcherCfg.TickBatchConfig.IntervalMs < 1000 {
		return logger.LogAndWrap("Tick batch interval is too low.", nil)
	}

	if batcherCfg.BookBatchConfig.BatchSize < 10 {
		return logger.LogAndWrap("Book batch config size is too low", nil)
	}

	if batcherCfg.BookBatchConfig.IntervalMs < 1000 {
		return logger.LogAndWrap("Book batch interval is too low.", nil)
	}

	kafkaCfg := cfg.KafkaConfig

	if len(kafkaCfg.BootstrapServers) == 0 {
		return logger.LogAndWrap("Bootstrap servers should not be blank", nil)
	}

	if strings.TrimSpace(kafkaCfg.TopicConfig.Tick) == "" {
		return logger.LogAndWrap("Tick topic should not be blank", nil)
	}

	if strings.TrimSpace(kafkaCfg.TopicConfig.Book) == "" {
		return logger.LogAndWrap("Book topic should not be blank", nil)
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

	if backpressureCfg.ConfirmSeconds <= 0 {
		return logger.LogAndWrap("Backpressure confirm seconds should be >0", nil)
	}

	if backpressureCfg.PollIntervalMs <= 0 {
		return logger.LogAndWrap("Backpressure poll interval millis should be >0", nil)
	}

	logger.Log.Info("Persistence config validated successfully")

	return nil
}

func LoadPostgresConfig() (*PostgresConfig, error) {
	u, err := mustEnv("POSTGRES_USER")
	if err != nil {
		return nil, err
	}

	p, err := mustEnv("POSTGRES_PASSWORD")
	if err != nil {
		return nil, err
	}

	d, err := mustEnv("POSTGRES_DB")
	if err != nil {
		return nil, err
	}

	return &PostgresConfig{
		Host:     getEnv("POSTGRES_HOST", "localhost"),
		Port:     getEnvInt("POSTGRES_PORT", 5432),
		User:     u,
		Password: p,
		Database: d,
		MaxConns: getEnvInt("POSTGRES_MAX_CONNS", 10),
	}, nil
}

func getEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func mustEnv(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return v, logger.LogAndWrap("Missing env variable for key. Exiting.", nil, "key", key)
	}
	return v, nil
}
