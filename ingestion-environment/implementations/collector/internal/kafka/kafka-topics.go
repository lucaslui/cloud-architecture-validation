package kafka

import (
	"context"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"

	"github.com/lucaslui/hems/collector/internal/config"
)

// Cria tópicos se ainda não existirem
func EnsureKafkaTopics(ctx context.Context, cfg *config.Config) error {
	bootstrap := cfg.KafkaBrokers[0]
	cfg.Logger.Printf("kafka: ensuring topics on bootstrap %s", bootstrap)

	// Conecta em qualquer broker para descobrir o controller
	conn, err := kafka.DialContext(ctx, "tcp", bootstrap)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Checa existência
	exists := func(topic string) bool {
		parts, err := conn.ReadPartitions(topic)
		return err == nil && len(parts) > 0
	}

	// Descobre e conecta no controller (onde se cria tópicos)
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	ctrlAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	ctrlConn, err := kafka.DialContext(ctx, "tcp", ctrlAddr)
	if err != nil {
		return err
	}
	defer ctrlConn.Close()

	// Principal
	if !exists(cfg.KafkaTopic) {
		cfg.Logger.Printf("kafka: creating topic %s (partitions=%d rf=%d)", cfg.KafkaTopic, cfg.KafkaTopicPartitions, cfg.KafkaReplicationFactor)
		if err := ctrlConn.CreateTopics(kafka.TopicConfig{
			Topic:             cfg.KafkaTopic,
			NumPartitions:     cfg.KafkaTopicPartitions,
			ReplicationFactor: cfg.KafkaReplicationFactor,
		}); err != nil {
			return err
		}
	} else {
		cfg.Logger.Printf("kafka: topic %s already exists — skipping", cfg.KafkaTopic)
	}

	// DLQ
	if !exists(cfg.KafkaDLQTopic) {
		cfg.Logger.Printf("kafka: creating topic %s (partitions=%d rf=%d)", cfg.KafkaDLQTopic, cfg.KafkaDLQPartitions, cfg.KafkaReplicationFactor)
		if err := ctrlConn.CreateTopics(kafka.TopicConfig{
			Topic:             cfg.KafkaDLQTopic,
			NumPartitions:     cfg.KafkaDLQPartitions,
			ReplicationFactor: cfg.KafkaReplicationFactor,
		}); err != nil {
			return err
		}
	} else {
		cfg.Logger.Printf("kafka: topic %s already exists — skipping", cfg.KafkaDLQTopic)
	}

	return nil
}
