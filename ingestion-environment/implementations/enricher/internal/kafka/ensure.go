package kafka

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"
)

const (
	defaultTopicReplication = int16(1)
	defaultOutRetentionMs   = "604800000"  // 7d
	defaultDLQRetentionMs   = "1209600000" // 14d
)

func toConfigEntries(m map[string]string) []kafka.ConfigEntry {
	if len(m) == 0 { return nil }
	out := make([]kafka.ConfigEntry, 0, len(m))
	for k, v := range m {
		val := v
		out = append(out, kafka.ConfigEntry{ConfigName: k, ConfigValue: val})
	}
	return out
}

func ensureTopic(ctx context.Context, broker, topic string, partitions int, rf int16, config map[string]string) error {
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil { return fmt.Errorf("dial broker: %w", err) }
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil { return fmt.Errorf("controller: %w", err) }

	ctrlAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	ctrlConn, err := kafka.DialContext(ctx, "tcp", ctrlAddr)
	if err != nil { return fmt.Errorf("dial controller: %w", err) }
	defer ctrlConn.Close()

	tc := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: int(rf),
		ConfigEntries:     toConfigEntries(config),
	}
	if err := ctrlConn.CreateTopics(tc); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "exists") {
			return fmt.Errorf("create topic %s: %w", topic, err)
		}
	}
	return nil
}

type EnsureTopicsArgs struct {
	Brokers            []string
	InputTopic         string
	OutputTopic        string
	OutTopicPartitions int
	DLQTopic           string
	DLQTopicPartitions int
}

func EnsureTopics(ctx context.Context, a EnsureTopicsArgs) {
	if len(a.Brokers) == 0 { log.Printf("[warn] nenhum broker para ensureTopics"); return }
	broker := a.Brokers[0]

	if err := ensureTopic(ctx, broker, a.OutputTopic, a.OutTopicPartitions, defaultTopicReplication,
		map[string]string{"cleanup.policy":"delete", "retention.ms": defaultOutRetentionMs}); err != nil {
		log.Printf("[warn] ensure output topic (%s): %v", a.OutputTopic, err)
	} else {
		log.Printf("[topics] ensured output topic=%s partitions=%d retention.ms=%s", a.OutputTopic, a.OutTopicPartitions, defaultOutRetentionMs)
	}

	if err := ensureTopic(ctx, broker, a.DLQTopic, a.DLQTopicPartitions, defaultTopicReplication,
		map[string]string{"cleanup.policy":"delete", "retention.ms": defaultDLQRetentionMs}); err != nil {
		log.Printf("[warn] ensure DLQ topic (%s): %v", a.DLQTopic, err)
	} else {
		log.Printf("[topics] ensured dlq topic=%s partitions=%d retention.ms=%s", a.DLQTopic, a.DLQTopicPartitions, defaultDLQRetentionMs)
	}

	if err := ensureTopic(ctx, broker, a.InputTopic, 3, defaultTopicReplication, nil); err != nil {
		log.Printf("[warn] ensure input topic (%s): %v", a.InputTopic, err)
	} else {
		log.Printf("[topics] ensured input topic=%s", a.InputTopic)
	}
}
