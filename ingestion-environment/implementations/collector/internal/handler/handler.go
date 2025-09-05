package handler

import (
	"context"
	"encoding/json"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"

	"github.com/lucaslui/hems/collector/internal/config"
	"github.com/lucaslui/hems/collector/internal/validate"
	kafkaSv"github.com/lucaslui/hems/collector/internal/kafka"
)

func HandleMessage(ctx context.Context, cfg *config.Config, prod *kafkaSv.KafkaProducer, msg mqtt.Message) {
	receivedAt := time.Now().UTC()
	payload := msg.Payload()

	// Log do recebimento no MQTT (amostra do payload)
	cfg.Logger.Printf(
		"mqtt rx: topic=%s qos=%d mid=%d retained=%v bytes=%d payload=%s",
		msg.Topic(), msg.Qos(), msg.MessageID(), msg.Retained(), len(payload), validate.Truncate(payload, 512),
	)

	m, err := validate.ValidatePayload(payload)
	if err != nil {
		cfg.Logger.Printf("invalid payload — sending to DLQ: %v | message: %s", err, validate.Truncate(payload, 512))
		// Envelope no DLQ
		dlq := map[string]any{
			"error":      err.Error(),
			"original":   json.RawMessage(payload),
			"topic":      msg.Topic(),
			"receivedAt": receivedAt.Format(time.RFC3339Nano),
		}
		buf, _ := json.Marshal(dlq)
		key := []byte("invalid")
		if err := prod.SendDLQ(ctx, key, buf); err != nil {
			cfg.Logger.Printf("kafka write error (dlq): %v", err)
		} else {
			cfg.Logger.Printf("dlq OK: topic=%s bytes=%d", cfg.KafkaDLQTopic, len(buf))
		}
		return
	}

	// Mensagem válida → encaminha JSON cru para Kafka com key=deviceId (se string)
	var key []byte
	if v, ok := m["deviceId"].(string); ok && v != "" {
		key = []byte(v)
	} else {
		key = []byte("unknown-device")
	}
	if err := prod.Send(ctx, key, payload, kafka.Header{
		Key:   "receivedAt",
		Value: []byte(receivedAt.Format(time.RFC3339Nano)),
	}); err != nil {
		cfg.Logger.Printf("kafka write error (main): %v", err)
		return
	}
	cfg.Logger.Printf("kafka OK: topic=%s key=%s bytes=%d", cfg.KafkaTopic, string(key), len(payload))
}
