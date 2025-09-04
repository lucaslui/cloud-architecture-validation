package main

import (
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
)

// Config via variáveis de ambiente
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

	Logger *log.Logger
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

func loadConfig() (*Config, error) {
	brokers := getenv("KAFKA_BROKERS", "localhost:9092")

	// Lê QoS (0..2) e retorna byte
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
		MQTTBrokerURL: getenv("MQTT_BROKER_URL", "tcp://vernemq:1883"),
		MQTTClientID:  getenv("MQTT_CLIENT_ID", "hems-collector"),
		MQTTUsername:  os.Getenv("MQTT_USERNAME"),
		MQTTPassword:  os.Getenv("MQTT_PASSWORD"),
		MQTTTopic:     getenv("MQTT_TOPIC", "ingestion/telemetry"),
		MQTTQoS:       getenvQoS("MQTT_QOS", 1),

		KafkaBrokers:           strings.Split(brokers, ","),
		KafkaTopic:             getenv("KAFKA_TOPIC", "raw-events-topic"),
		KafkaDLQTopic:          getenv("KAFKA_DLQ_TOPIC", "raw-events-dlq"),
		KafkaTopicPartitions:   getenvInt("KAFKA_TOPIC_PARTITIONS", 3),
		KafkaDLQPartitions:     getenvInt("KAFKA_DLQ_PARTITIONS", 1),
		KafkaReplicationFactor: getenvInt("KAFKA_REPLICATION_FACTOR", 1),

		Logger: log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds),
	}
	if len(cfg.KafkaBrokers) == 0 || cfg.KafkaBrokers[0] == "" {
		return nil, errors.New("KAFKA_BROKERS must not be empty")
	}
	return cfg, nil
}
