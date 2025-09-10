package config

import (
	"fmt"
	"os"
	"strings"
)

const (
	DefaultKafkaBrokers = "localhost:9092"
	DefaultGroupID      = "validator"
	DefaultInputTopic   = "raw-events-topic"
	DefaultOutputTopic  = "validated-events-topic"
	DefaultDLQTopic     = "validated-events-dlq"

	DefaultOutTopicPartitions = 3
	DefaultDLQTopicPartitions = 1

	AckBatchSize = 500

	DefaultRedisAddr           = "localhost:6379"
	DefaultRedisNamespace      = "schema"
	DefaultRedisInvalidateChan = "schemas:invalidate"

	DefaultProcessingWorkers = 8
)

type Config struct {
	// Kafka
	Brokers     []string
	GroupID     string
	InputTopic  string
	OutputTopic string
	DLQTopic    string

	OutTopicPartitions int
	DLQTopicPartitions int

	AckBatchSize int

	// Redis (schema registry)
	RedisAddr           string
	RedisPassword       string
	RedisDB             int
	RedisNamespace      string
	RedisUsePubSub      bool
	RedisInvalidateChan string

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

func envOrDefaultBool(key string, def bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "y":
		return true
	case "0", "false", "no", "n":
		return false
	default:
		return def
	}
}

func Load() Config {
	return Config{
		Brokers:     strings.Split(envOrDefault("KAFKA_BROKERS", DefaultKafkaBrokers), ","),
		GroupID:     envOrDefault("KAFKA_GROUP_ID", DefaultGroupID),
		InputTopic:  envOrDefault("KAFKA_INPUT_TOPIC", DefaultInputTopic),
		OutputTopic: envOrDefault("KAFKA_OUTPUT_TOPIC", DefaultOutputTopic),
		DLQTopic:    envOrDefault("KAFKA_DLQ_TOPIC", DefaultDLQTopic),

		OutTopicPartitions: envOrDefaultInt("OUT_TOPIC_PARTITIONS", DefaultOutTopicPartitions),
		DLQTopicPartitions: envOrDefaultInt("KAFKA_DLQ_TOPIC_PARTITIONS", DefaultDLQTopicPartitions),

		AckBatchSize: envOrDefaultInt("ACK_BATCH_SIZE", AckBatchSize),

		RedisAddr:           envOrDefault("REDIS_ADDR", DefaultRedisAddr),
		RedisPassword:       os.Getenv("REDIS_PASSWORD"),
		RedisDB:             envOrDefaultInt("REDIS_DB", 0),
		RedisNamespace:      envOrDefault("REDIS_NAMESPACE", DefaultRedisNamespace),
		RedisUsePubSub:      envOrDefaultBool("REDIS_USE_PUBSUB", true),
		RedisInvalidateChan: envOrDefault("REDIS_INVALIDATE_CHANNEL", DefaultRedisInvalidateChan),

		ProcessingWorkers: envOrDefaultInt("PROCESSING_WORKERS", DefaultProcessingWorkers),
	}
}
