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

func HandleMessage(ctx context.Context, cfg *config.Config, logger *log.Logger, prod *broker.KafkaClient, disp *broker.KafkaDispatcher, msg mqtt.Message) {
	collectedAt := time.Now().UTC()
	raw := msg.Payload()

	agg, err := validate.ValidateAggregated(raw)
	if err != nil {
		logger.Printf("[error] invalid aggregated payload — DLQ: %v | message: %s", err, validate.Truncate(raw, 512))
		dlq := map[string]any{
			"error":      err.Error(),
			"original":   json.RawMessage(raw),
			"topic":      msg.Topic(),
			"receivedAt": collectedAt.Format(time.RFC3339Nano),
		}
		buf, _ := json.Marshal(dlq)
		_ = prod.SendDLQ(ctx, []byte("invalid"), buf)
		return
	}

	for _, item := range agg.Payload {
		out := model.OutboundCollectorEnvelope{
			InboundCollectorEnvelope: item,
			Metadata: model.OutboundCollectorMetadata{
				ControllerID:        agg.ControllerID,
				ControllerTimestamp: agg.ControllerTimestamp,
				EventID:             uuid.NewString(),
				CollectedAt:         collectedAt,
			},
		}
		value, err := json.Marshal(out)
		if err != nil {
			logger.Printf("[error] json marshal error: %v", err)
			continue
		}

		key := []byte(item.DeviceID)
		if len(key) == 0 {
			key = []byte("unknown-device")
		}

		disp.Enqueue(kafka.Message{
			Key:     key,
			Value:   value,
			Headers: []kafka.Header{{Key: "receivedAt", Value: []byte(collectedAt.Format(time.RFC3339Nano))}},
		})
	}

	// logger.Printf("[info] aggregated OK: controller=%s items=%d", agg.ControllerID, len(agg.Payload))
}
