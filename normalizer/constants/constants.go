package constants

type Config struct {
	KafkaConfig *KafkaConfig `yaml:"kafka"`
}

type KafkaConfig struct {
	Brokers                    []string `yaml:"brokers"`
	Topics                     []string `yaml:"topics"`
	ConsumerGroup              string   `yaml:"consumer_group"`
	MaxBufferRecords           int      `yaml:"max_buffer_records"`
	CommitOffsetIntervalMillis int      `yaml:"commit_offset_interval_ms"`
}

const (
	ConfigFilePath           string = "./config/config.yaml"
	DefaultConsumerGroupName string = "normalizer-group-1"
)
