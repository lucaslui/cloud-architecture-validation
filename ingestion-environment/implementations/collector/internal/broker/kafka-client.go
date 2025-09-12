package broker

import (
	"context"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lucaslui/hems/collector/internal/config"
)

type KafkaClient struct {
	MainProducer *kafka.Writer
	DQLProducer  *kafka.Writer
}

func NewKafkaClient(cfg *config.Config) *KafkaClient {
	return &KafkaClient{
		MainProducer: NewKafkaProducer(cfg, cfg.KafkaTopic),
		DQLProducer:  NewKafkaProducer(cfg, cfg.KafkaDLQTopic),
	}
}

func NewKafkaProducer(cfg *config.Config, topic string) *kafka.Writer {
	balancer := &kafka.Hash{}

	return &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers...),
		Topic:    topic,
		Balancer: balancer,

		BatchSize:    cfg.KafkaBatchSize,
		BatchBytes:   cfg.KafkaBatchBytes,
		BatchTimeout: time.Duration(cfg.KafkaBatchTimeoutMs) * time.Millisecond,

		RequiredAcks: parseAcks(cfg.KafkaRequiredAcks),
		MaxAttempts:  cfg.KafkaMaxAttempts,
		Async:        true,
		Compression:  parseCompression(cfg.KafkaCompression),
	}
}

func (p *KafkaClient) Close() {
	_ = p.MainProducer.Close()
	_ = p.DQLProducer.Close()
}

func (p *KafkaClient) Send(ctx context.Context, key, value []byte, headers ...kafka.Header) error {
	return p.MainProducer.WriteMessages(ctx, kafka.Message{
		Key:     key,
		Value:   value,
		Headers: headers,
	})
}

func (p *KafkaClient) SendDLQ(ctx context.Context, key, value []byte, headers ...kafka.Header) error {
	return p.DQLProducer.WriteMessages(ctx, kafka.Message{
		Key:     key,
		Value:   value,
		Headers: headers,
	})
}

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
