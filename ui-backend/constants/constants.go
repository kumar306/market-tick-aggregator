package constants

const (
	EnvFile    string = "../.env"
	ConfigFile string = "./config/config.yaml"
)

type Config struct {
	KafkaConfig *KafkaConfig `yaml:"kafka"`
}

type KafkaConfig struct {
	BootstrapServers []string    `yaml:"bootstrap_servers"`
	TopicConfig      TopicConfig `yaml:"topics"`
	ConsumerGroup    string      `yaml:"consumer_group"`
	MaxBufferRecords int         `yaml:"max_buffer_records"`
}

type TopicConfig struct {
	Ticks string `yaml:"ticks"`
	Book  string `yaml:"book"`
}
