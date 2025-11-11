package constants

type Config struct {
	KafkaConfig *KafkaConfig `yaml:"kafka"`
	WorkerCount int          `yaml:"worker_count"`
}

type KafkaConfig struct {
	Brokers                    []string `yaml:"brokers"`
	Topics                     []string `yaml:"topics"`
	ConsumerGroup              string   `yaml:"consumer_group"`
	MaxBufferRecords           int      `yaml:"max_buffer_records"`
	CommitOffsetIntervalMillis int      `yaml:"commit_offset_interval_ms"`
}

type Header struct {
	Exchange string `json:"exchange"`
	Channel  string `json:"channel"`
}

const (
	ConfigFilePath           string = "./config/config.yaml"
	DefaultConsumerGroupName string = "normalizer-group-1"
)
