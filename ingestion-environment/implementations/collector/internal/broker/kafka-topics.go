package broker

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"

	"github.com/lucaslui/hems/collector/internal/config"
)

func EnsureKafkaTopics(ctx context.Context, cfg *config.Config, logger *log.Logger) error {
	bootstrap := cfg.KafkaBrokers[0]

	logger.Printf("[info] kafka ensuring topics on bootstrap %s", bootstrap)

	conn, err := kafka.DialContext(ctx, "tcp", bootstrap)
	if err != nil {
		return err
	}
	defer conn.Close()

	exists := func(topic string) bool {
		parts, err := conn.ReadPartitions(topic)
		return err == nil && len(parts) > 0
	}

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

	if !exists(cfg.KafkaTopic) {
		logger.Printf("[info] kafka creating topic %s (partitions=%d rf=%d)", cfg.KafkaTopic, cfg.KafkaTopicPartitions, cfg.KafkaReplicationFactor)
		if err := ctrlConn.CreateTopics(kafka.TopicConfig{
			Topic:             cfg.KafkaTopic,
			NumPartitions:     cfg.KafkaTopicPartitions,
			ReplicationFactor: cfg.KafkaReplicationFactor,
			ConfigEntries: []kafka.ConfigEntry{
				{ConfigName: "compression.type", ConfigValue: cfg.KafkaCompression},
				{ConfigName: "retention.ms", ConfigValue: fmt.Sprintf("%d", cfg.KafkaRetentionMs)},
			},
		}); err != nil {
			return err
		}
	} else {
		logger.Printf("[info] kafka topic %s already exists — skipping", cfg.KafkaTopic)
	}

	if !exists(cfg.KafkaDLQTopic) {
		logger.Printf("[info] kafka creating topic %s (partitions=%d rf=%d)", cfg.KafkaDLQTopic, cfg.KafkaDLQPartitions, cfg.KafkaReplicationFactor)
		if err := ctrlConn.CreateTopics(kafka.TopicConfig{
			Topic:             cfg.KafkaDLQTopic,
			NumPartitions:     cfg.KafkaDLQPartitions,
			ReplicationFactor: cfg.KafkaReplicationFactor,
			ConfigEntries: []kafka.ConfigEntry{
				{ConfigName: "compression.type", ConfigValue: cfg.KafkaCompression},
				{ConfigName: "retention.ms", ConfigValue: fmt.Sprintf("%d", cfg.KafkaRetentionMs)},
			},
		}); err != nil {
			return err
		}
	} else {
		logger.Printf("[info] kafka topic %s already exists — skipping", cfg.KafkaDLQTopic)
	}

	return nil
}
