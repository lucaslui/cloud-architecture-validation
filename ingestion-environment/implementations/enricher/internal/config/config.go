package config

import (
	"fmt"
	"os"
	"strings"
)

const (
	DefaultKafkaBrokers = "localhost:9092"
	DefaultGroupID      = "enricher-validator"
	DefaultInputTopic   = "validated-events-topic"
	DefaultOutputTopic  = "enriched-events-topic"
	DefaultDLQTopic     = "enriched-events-dlq"

	DefaultOutTopicPartitions = 3
	DefaultDLQTopicPartitions = 1

	DefaultContextStoreJSON = "./device-context.json"

	DefaultProcessingWorkers = 8
	DefaultAckBatchSize      = 500
)

type Config struct {
	Brokers     []string
	GroupID     string
	InputTopic  string
	OutputTopic string
	DLQTopic    string

	OutTopicPartitions int
	DLQTopicPartitions int

	ContextStorePath string

	AckBatchSize      int
	ProcessingWorkers int
}

func envOrDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}
func envOrDefaultInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		var n int
		fmt.Sscanf(v, "%d", &n)
		if n != 0 || v == "0" {
			return n
		}
	}
	return def
}

func LoadConfig() Config {
	return Config{
		Brokers:     strings.Split(envOrDefault("KAFKA_BROKERS", DefaultKafkaBrokers), ","),
		GroupID:     envOrDefault("KAFKA_GROUP_ID", DefaultGroupID),
		InputTopic:  envOrDefault("KAFKA_INPUT_TOPIC", DefaultInputTopic),
		OutputTopic: envOrDefault("KAFKA_OUTPUT_TOPIC", DefaultOutputTopic),
		DLQTopic:    envOrDefault("KAFKA_DLQ_TOPIC", DefaultDLQTopic),

		OutTopicPartitions: envOrDefaultInt("OUT_TOPIC_PARTITIONS", DefaultOutTopicPartitions),
		DLQTopicPartitions: envOrDefaultInt("KAFKA_DLQ_TOPIC_PARTITIONS", DefaultDLQTopicPartitions),

		ContextStorePath: envOrDefault("CONTEXT_STORE_PATH", DefaultContextStoreJSON),

		AckBatchSize:      envOrDefaultInt("ACK_BATCH_SIZE", DefaultAckBatchSize),
		ProcessingWorkers: envOrDefaultInt("PROCESSING_WORKERS", DefaultProcessingWorkers),
	}
}
