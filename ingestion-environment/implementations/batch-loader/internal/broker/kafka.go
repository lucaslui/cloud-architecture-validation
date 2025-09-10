package broker

import (
	"time"

	"github.com/segmentio/kafka-go"
)

func NewKafkaReader(brokers, groupID, topic string, minBytes, maxBytes, maxWaitMs int) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{brokers},
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       minBytes,
		MaxBytes:       maxBytes,
		MaxWait:        time.Duration(maxWaitMs) * time.Millisecond,
		QueueCapacity:  2048,  // NOVO: mais mensagens em buffer
		ReadLagInterval: -1,   // NOVO: desativa cálculo de lag (menos overhead)
		// não force StartOffset; use o offset committed do grupo
	})
}
