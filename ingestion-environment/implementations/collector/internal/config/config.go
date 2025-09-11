package config

import (
	"errors"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
)

type Config struct {
	MQTTBrokerURL string
	MQTTClientID  string
	MQTTUsername  string
	MQTTPassword  string
	MQTTTopic     string
	MQTTQoS       byte

	KafkaBrokers           []string
	KafkaTopic             string
	KafkaDLQTopic          string
	KafkaTopicPartitions   int
	KafkaDLQPartitions     int
	KafkaReplicationFactor int
	KafkaBatchSize         int
	KafkaBatchBytes        int64
	KafkaBatchTimeoutMs    int
	KafkaCompression       string
	KafkaRequiredAcks      string
	KafkaMaxAttempts       int

	Logger *log.Logger

	ProcessingWorkers int
	WorkerQueueSize   int

	DispatcherCapacity    int
	DispatcherMaxBatch    int
	DispatcherTickMs      int
	WorkQueueStrategy     string // "drop" or "block"
	WorkQueueEnqTimeoutMs int
	MQTTChannelDepth      uint
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getenvUInt(key string, fallback uint) uint {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseUint(v, 10, 32); err == nil {
			return uint(n)
		}
	}
	return fallback
}

func getenvInt64(key string, fallback int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return fallback
}

func LoadConfig() (*Config, error) {
	brokers := getenv("KAFKA_BROKERS", "localhost:9092")

	getenvQoS := func(key string, fallback byte) byte {
		val := os.Getenv(key)
		if val == "" {
			return fallback
		}
		if n, err := strconv.Atoi(val); err == nil {
			if n < 0 {
				n = 0
			}
			if n > 2 {
				n = 2
			}
			return byte(n)
		}
		return fallback
	}

	cfg := &Config{
		MQTTBrokerURL:    getenv("MQTT_BROKER_URL", "tcp://vernemq:1883"),
		MQTTClientID:     getenv("MQTT_CLIENT_ID", "hems-collector"),
		MQTTUsername:     os.Getenv("MQTT_USERNAME"),
		MQTTPassword:     os.Getenv("MQTT_PASSWORD"),
		MQTTTopic:        getenv("MQTT_TOPIC", "ingestion/telemetry"),
		MQTTQoS:          getenvQoS("MQTT_QOS", 1),
		MQTTChannelDepth: getenvUInt("MQTT_CHANNEL_DEPTH", 5000),

		KafkaBrokers:           strings.Split(brokers, ","),
		KafkaTopic:             getenv("KAFKA_TOPIC", "raw-events-topic"),
		KafkaDLQTopic:          getenv("KAFKA_DLQ_TOPIC", "raw-events-dlq"), // <-- fix
		KafkaTopicPartitions:   getenvInt("KAFKA_TOPIC_PARTITIONS", 3),
		KafkaDLQPartitions:     getenvInt("KAFKA_DLQ_PARTITIONS", 1),
		KafkaReplicationFactor: getenvInt("KAFKA_REPLICATION_FACTOR", 1),
		KafkaBatchSize:         getenvInt("KAFKA_BATCH_SIZE", 1000),
		KafkaBatchBytes:        getenvInt64("KAFKA_BATCH_BYTES", 1<<20), // 1MB
		KafkaBatchTimeoutMs:    getenvInt("KAFKA_BATCH_TIMEOUT_MS", 5),
		KafkaCompression:       getenv("KAFKA_COMPRESSION", "snappy"), // none, gzip, snappy, lz4, zstd
		KafkaRequiredAcks:      getenv("KAFKA_REQUIRED_ACKS", "one"),  // none, one, all
		KafkaMaxAttempts:       getenvInt("KAFKA_MAX_ATTEMPTS", 10),

		Logger: log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds),

		ProcessingWorkers: getenvInt("PROCESSING_WORKERS", runtime.NumCPU()*2),
		WorkerQueueSize:   getenvInt("WORKER_QUEUE_SIZE", 10000),

		DispatcherCapacity:    getenvInt("DISPATCHER_CAPACITY", 10000),
		DispatcherMaxBatch:    getenvInt("DISPATCHER_MAX_BATCH", 2000),
		DispatcherTickMs:      getenvInt("DISPATCHER_TICK_MS", 5),
		WorkQueueStrategy:     getenv("WORK_QUEUE_STRATEGY", "drop"),
		WorkQueueEnqTimeoutMs: getenvInt("WORK_QUEUE_ENQ_TIMEOUT_MS", 0),
	}

	if len(cfg.KafkaBrokers) == 0 || cfg.KafkaBrokers[0] == "" {
		return nil, errors.New("KAFKA_BROKERS must not be empty")
	}

	return cfg, nil
}
