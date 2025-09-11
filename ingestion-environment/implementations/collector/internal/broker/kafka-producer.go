package broker

import (
	"context"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lucaslui/hems/collector/internal/config"
)

func parseCompression(s string) kafka.Compression {
	switch strings.ToLower(s) {
	case "", "none", "no", "off", "0":
		return kafka.Compression(0)
	case "gzip":
		return kafka.Gzip
	case "snappy":
		return kafka.Snappy
	case "lz4":
		return kafka.Lz4
	case "zstd":
		return kafka.Zstd
	default:
		return kafka.Snappy
	}
}

func parseAcks(s string) kafka.RequiredAcks {
	switch strings.ToLower(s) {
	case "none":
		return kafka.RequireNone
	case "all":
		return kafka.RequireAll
	default:
		return kafka.RequireOne
	}
}

type KafkaProducer struct {
	main *kafka.Writer
	dlq  *kafka.Writer
}

func NewKafkaProducer(cfg *config.Config) *KafkaProducer {
	balancer := &kafka.Hash{}

	main := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers...),
		Topic:    cfg.KafkaTopic,
		Balancer: balancer,

		BatchSize:    cfg.KafkaBatchSize,
		BatchBytes:   cfg.KafkaBatchBytes,
		BatchTimeout: time.Duration(cfg.KafkaBatchTimeoutMs) * time.Millisecond,

		RequiredAcks: parseAcks(cfg.KafkaRequiredAcks),
		MaxAttempts:  cfg.KafkaMaxAttempts,
		Async:        true,
		Compression:  parseCompression(cfg.KafkaCompression),
	}

	dlq := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers...),
		Topic:    cfg.KafkaDLQTopic,
		Balancer: balancer,

		BatchSize:    max(1, cfg.KafkaBatchSize/5),
		BatchBytes:   int64(max(1, int(cfg.KafkaBatchBytes)/2)),
		BatchTimeout: 10 * time.Millisecond,

		RequiredAcks: parseAcks(cfg.KafkaRequiredAcks),
		MaxAttempts:  cfg.KafkaMaxAttempts,
		Async:        true,
		Compression:  parseCompression(cfg.KafkaCompression),
	}

	return &KafkaProducer{main: main, dlq: dlq}
}

func (p *KafkaProducer) Close(ctx context.Context) {
	_ = p.main.Close()
	_ = p.dlq.Close()
}

func (p *KafkaProducer) Send(ctx context.Context, key, value []byte, headers ...kafka.Header) error {
	return p.main.WriteMessages(ctx, kafka.Message{
		Key:     key,
		Value:   value,
		Headers: headers,
	})
}

func (p *KafkaProducer) SendDLQ(ctx context.Context, key, value []byte, headers ...kafka.Header) error {
	return p.dlq.WriteMessages(ctx, kafka.Message{
		Key:     key,
		Value:   value,
		Headers: headers,
	})
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
