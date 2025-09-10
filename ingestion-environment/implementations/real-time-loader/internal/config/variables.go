package config

import (
	"os"
	"strconv"
)

type Config struct {
	KafkaBrokers    string
	KafkaGroupID    string
	KafkaInputTopic string

	InfluxURL    string
	InfluxToken  string
	InfluxOrg    string
	InfluxBucket string

	// já existiam/foram sugeridos antes
	ProcessingWorkers int
	AckBatchSize      int
	KafkaMinBytes     int
	KafkaMaxBytes     int
	KafkaMaxWaitMs    int

	// NOVO: batching assíncrono do Influx
	InfluxBatchSize       int // ex.: 5000
	InfluxFlushIntervalMs int // ex.: 100 (ms)
}

func getenvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			return i
		}
	}
	return def
}

func LoadEnvVariables() *Config {
	return &Config{
		KafkaBrokers:    os.Getenv("KAFKA_BROKERS"),
		KafkaGroupID:    os.Getenv("KAFKA_GROUP_ID"),
		KafkaInputTopic: os.Getenv("KAFKA_INPUT_TOPIC"),

		InfluxURL:    os.Getenv("INFLUX_URL"),
		InfluxToken:  os.Getenv("INFLUX_TOKEN"),
		InfluxOrg:    os.Getenv("INFLUX_ORG"),
		InfluxBucket: os.Getenv("INFLUX_BUCKET"),

		// defaults sensatos
		ProcessingWorkers: getenvInt("WORKERS", 8),
		AckBatchSize:      getenvInt("ACK_BATCH_SIZE", 500),
		KafkaMinBytes:     getenvInt("KAFKA_MIN_BYTES", 10_000),     // 10KB
		KafkaMaxBytes:     getenvInt("KAFKA_MAX_BYTES", 10_000_000), // 10MB
		KafkaMaxWaitMs:    getenvInt("KAFKA_MAX_WAIT_MS", 50),

		InfluxBatchSize:       getenvInt("INFLUX_BATCH_SIZE", 5000),
		InfluxFlushIntervalMs: getenvInt("INFLUX_FLUSH_INTERVAL_MS", 100),
	}
}
