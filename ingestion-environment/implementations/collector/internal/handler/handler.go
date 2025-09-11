package handler

import (
	"context"
	"encoding/json"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/lucaslui/hems/collector/internal/broker"
	"github.com/lucaslui/hems/collector/internal/config"
	"github.com/lucaslui/hems/collector/internal/model"
	"github.com/lucaslui/hems/collector/internal/validate"
)

func HandleMessage(ctx context.Context, cfg *config.Config, logger *log.Logger, prod *broker.KafkaProducer, disp *broker.KafkaDispatcher, msg mqtt.Message) {
	collectedAt := time.Now().UTC()
	raw := msg.Payload()

	// cfg.Logger.Printf("mqtt rx: topic=%s qos=%d mid=%d retained=%v bytes=%d payload=%s",
	// 	msg.Topic(), msg.Qos(), msg.MessageID(), msg.Retained(), len(raw), validate.Truncate(raw, 512),
	// )

	env, err := validate.ValidatePayload(raw)

	if err != nil {
		logger.Printf("[error] invalid payload — sending to DLQ: %v | message: %s", err, validate.Truncate(raw, 512))
		dlq := map[string]any{
			"error":      err.Error(),
			"original":   json.RawMessage(raw),
			"topic":      msg.Topic(),
			"receivedAt": collectedAt.Format(time.RFC3339Nano),
		}
		buf, _ := json.Marshal(dlq)
		if err := prod.SendDLQ(ctx, []byte("invalid"), buf); err != nil {
			logger.Printf("[error] kafka write error (dlq): %v", err)
		} else {
			logger.Printf("[info] dlq OK: topic=%s bytes=%d", cfg.KafkaDLQTopic, len(buf))
		}
		return
	}

	out := model.OutboundCollectorEnvelope{
		InboundCollectorEnvelope: env,
		Metadata: model.OutboundCollectorMetadata{
			EventID:     uuid.NewString(),
			CollectedAt: collectedAt,
		},
	}

	value, err := json.Marshal(out)

	if err != nil {
		logger.Printf("[error] json marshal error: %v", err)
		return
	}

	key := []byte(env.DeviceID)

	if len(key) == 0 {
		key = []byte("unknown-device")
	}

	disp.Enqueue(kafka.Message{
		Key:     key,
		Value:   value,
		Headers: []kafka.Header{{Key: "receivedAt", Value: []byte(collectedAt.Format(time.RFC3339Nano))}},
	})
}
