package broker

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lucaslui/hems/validator/internal/config"
)

func dialAnyBroker(ctx context.Context, brokers []string, perAttempt time.Duration, logger *log.Logger) (*kafka.Conn, string, error) {
	var lastErr error
	for _, b := range brokers {
		dctx, cancel := context.WithTimeout(ctx, perAttempt)
		conn, err := kafka.DialContext(dctx, "tcp", b)
		cancel()
		if err == nil {
			logger.Printf("[info] kafka connected to bootstrap %s", b)
			return conn, b, nil
		}
		lastErr = err
		logger.Printf("[warn] kafka cannot connect to %s: %v (trying next)", b, err)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no brokers provided")
	}
	return nil, "", lastErr
}

func dialController(ctx context.Context, ctrl kafka.Broker, perAttempt time.Duration) (*kafka.Conn, error) {
	addr := net.JoinHostPort(ctrl.Host, strconv.Itoa(ctrl.Port))
	dctx, cancel := context.WithTimeout(ctx, perAttempt)
	defer cancel()
	return kafka.DialContext(dctx, "tcp", addr)
}

func EnsureKafkaTopics(ctx context.Context, cfg *config.Config, logger *log.Logger) error {
	const perAttempt = 5 * time.Second
	conn, bootstrap, err := dialAnyBroker(ctx, cfg.KafkaBrokers, perAttempt, logger)
	if err != nil {
		return fmt.Errorf("bootstrap connect failed (tried %v): %w", cfg.KafkaBrokers, err)
	}
	defer conn.Close()

	exists := func(topic string) bool {
		parts, err := conn.ReadPartitions(topic)
		return err == nil && len(parts) > 0
	}

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("read controller from %s failed: %w", bootstrap, err)
	}

	ctrlConn, err := dialController(ctx, controller, perAttempt)
	if err != nil {
		_ = conn.Close()
		conn2, bootstrap2, err2 := dialAnyBroker(ctx, cfg.KafkaBrokers, perAttempt, logger)
		if err2 != nil {
			return fmt.Errorf("controller connect failed (fallback): %w (first error: %v)", err2, err)
		}
		defer conn2.Close()

		controller2, err2 := conn2.Controller()
		if err2 != nil {
			return fmt.Errorf("read controller (fallback) failed from %s: %w (first error: %v)", bootstrap2, err2, err)
		}
		ctrlConn, err = dialController(ctx, controller2, perAttempt)
		if err != nil {
			return fmt.Errorf("controller dial failed (second attempt): %w (first error: %v)", err, err)
		}
	}
	defer ctrlConn.Close()

	if !exists(cfg.KafkaReaderTopic) {
		logger.Printf("[info] kafka creating input topic %s (partitions=%d rf=%d)", cfg.KafkaReaderTopic, cfg.KafkaTopicPartitions, cfg.KafkaReplicationFactor)
		if err := ctrlConn.CreateTopics(kafka.TopicConfig{
			Topic:             cfg.KafkaReaderTopic,
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
		logger.Printf("[info] kafka input topic %s already exists — skipping", cfg.KafkaReaderTopic)
	}

	if !exists(cfg.KafkaWriterTopic) {
		logger.Printf("[info] kafka creating output topic %s (partitions=%d rf=%d)", cfg.KafkaWriterTopic, cfg.KafkaTopicPartitions, cfg.KafkaReplicationFactor)
		if err := ctrlConn.CreateTopics(kafka.TopicConfig{
			Topic:             cfg.KafkaWriterTopic,
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
		logger.Printf("[info] kafka output topic %s already exists — skipping", cfg.KafkaWriterTopic)
	}

	if !exists(cfg.KafkaDLQTopic) {
		logger.Printf("[info] kafka creating DLQ topic %s (partitions=%d rf=%d)", cfg.KafkaDLQTopic, cfg.KafkaDLQPartitions, cfg.KafkaReplicationFactor)
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
		logger.Printf("[info] kafka DLQ topic %s already exists — skipping", cfg.KafkaDLQTopic)
	}

	return nil
}
