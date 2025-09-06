package handler

import (
	"context"
	"encoding/json"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/lucaslui/hems/collector/internal/config"
	kafkaSv "github.com/lucaslui/hems/collector/internal/kafka"
	"github.com/lucaslui/hems/collector/internal/model"
	"github.com/lucaslui/hems/collector/internal/validate"
)

func HandleMessage(ctx context.Context, cfg *config.Config, prod *kafkaSv.KafkaProducer, msg mqtt.Message) {
	collectedAt := time.Now().UTC()
	raw := msg.Payload()

	cfg.Logger.Printf(
		"mqtt rx: topic=%s qos=%d mid=%d retained=%v bytes=%d payload=%s",
		msg.Topic(), msg.Qos(), msg.MessageID(), msg.Retained(), len(raw), validate.Truncate(raw, 512),
	)

	env, err := validate.ValidatePayload(raw)
	if err != nil {
		cfg.Logger.Printf("invalid payload — sending to DLQ: %v | message: %s", err, validate.Truncate(raw, 512))
		dlq := map[string]any{
			"error":      err.Error(),
			"original":   json.RawMessage(raw),
			"topic":      msg.Topic(),
			"receivedAt": collectedAt.Format(time.RFC3339Nano),
		}
		buf, _ := json.Marshal(dlq)
		if err := prod.SendDLQ(ctx, []byte("invalid"), buf); err != nil {
			cfg.Logger.Printf("kafka write error (dlq): %v", err)
		} else {
			cfg.Logger.Printf("dlq OK: topic=%s bytes=%d", cfg.KafkaDLQTopic, len(buf))
		}
		return
	}

	// Enriquecimento com Metadata e serialização final
	out := model.OutboundEnvelope{
		InboundEnvelope: env,
		Metadata: model.Metadata{
			EventID:     uuid.NewString(),
			CollectedAt: collectedAt,
		},
	}
	value, err := json.Marshal(out)
	if err != nil {
		cfg.Logger.Printf("json marshal error: %v", err)
		return
	}

	// Chave = deviceId (ou "unknown-device")
	key := []byte(env.DeviceID)
	if len(key) == 0 {
		key = []byte("unknown-device")
	}

	if err := prod.Send(ctx, key, value, kafka.Header{
		Key:   "receivedAt",
		Value: []byte(collectedAt.Format(time.RFC3339Nano)),
	}); err != nil {
		cfg.Logger.Printf("kafka write error (main): %v", err)
		return
	}
	cfg.Logger.Printf("kafka OK: topic=%s key=%s bytes=%d", cfg.KafkaTopic, string(key), len(value))
}
