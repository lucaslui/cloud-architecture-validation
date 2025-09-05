package config

import (
	"os"
	"strings"
	"fmt"
)

const (
	DefaultKafkaBrokers  = "localhost:9092"
	DefaultGroupID       = "enricher-validator"
	DefaultInputTopic    = "raw-events-topic"
	DefaultOutputTopic   = "enriched-events-topic"
	DefaultDLQTopic      = "validation-dlq-topic"

	DefaultOutTopicPartitions = 3
	DefaultDLQTopicPartitions = 1

	DefaultRedisAddr           = "localhost:6379"
	DefaultRedisNamespace      = "schema"
	DefaultRedisInvalidateChan = "schemas:invalidate"

	DefaultContextStoreJSON = "./device-context.json"
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

	// Redis (schema registry)
	RedisAddr           string
	RedisPassword       string
	RedisDB             int
	RedisNamespace      string
	RedisUsePubSub      bool
	RedisInvalidateChan string

	// Enriquecimento
	ContextStorePath string
}

func envOrDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" { return v }
	return def
}
func envOrDefaultInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		var n int; fmt.Sscanf(v, "%d", &n)
		if n != 0 || v == "0" { return n }
	}
	return def
}
func envOrDefaultBool(key string, def bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1","true","yes","y": return true
	case "0","false","no","n": return false
	default: return def
	}
}

func Load() Config {
	return Config{
		Brokers:            strings.Split(envOrDefault("KAFKA_BROKERS", DefaultKafkaBrokers), ","),
		GroupID:            envOrDefault("KAFKA_GROUP_ID", DefaultGroupID),
		InputTopic:         envOrDefault("INPUT_TOPIC", DefaultInputTopic),
		OutputTopic:        envOrDefault("OUTPUT_TOPIC", DefaultOutputTopic),
		DLQTopic:           envOrDefault("DLQ_TOPIC", DefaultDLQTopic),

		OutTopicPartitions: envOrDefaultInt("OUT_TOPIC_PARTITIONS", DefaultOutTopicPartitions),
		DLQTopicPartitions: envOrDefaultInt("DLQ_TOPIC_PARTITIONS", DefaultDLQTopicPartitions),

		RedisAddr:           envOrDefault("REDIS_ADDR", DefaultRedisAddr),
		RedisPassword:       os.Getenv("REDIS_PASSWORD"),
		RedisDB:             envOrDefaultInt("REDIS_DB", 0),
		RedisNamespace:      envOrDefault("REDIS_NAMESPACE", DefaultRedisNamespace),
		RedisUsePubSub:      envOrDefaultBool("REDIS_USE_PUBSUB", true),
		RedisInvalidateChan: envOrDefault("REDIS_INVALIDATE_CHANNEL", DefaultRedisInvalidateChan),

		ContextStorePath: envOrDefault("CONTEXT_STORE_PATH", DefaultContextStoreJSON),
	}
}
