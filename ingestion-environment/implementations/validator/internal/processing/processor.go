package processing

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/lucaslui/hems/validator/internal/config"
	"github.com/lucaslui/hems/validator/internal/model"
	"github.com/lucaslui/hems/validator/internal/registry"
)

type Processor struct {
	cfg       *config.Config
	reg       registry.SchemaRegistry
	inTopic   string
	outWriter *kafka.Writer
	dlqWriter *kafka.Writer
}

func NewProcessor(cfg *config.Config, reg registry.SchemaRegistry, out, dlq *kafka.Writer) *Processor {
	return &Processor{
		cfg: cfg, reg: reg, inTopic: cfg.KafkaInputTopic, outWriter: out, dlqWriter: dlq,
	}
}

func (p *Processor) Process(ctx context.Context, msg kafka.Message) (latency time.Duration, err error) {
	start := time.Now()
	stage := "decode_envelope"

	var env model.InboundValidatorEnvelope

	if err := json.Unmarshal(msg.Value, &env); err != nil {
		p.emitDLQ(ctx, model.OutboundValidatorDLQEnvelope{
			Error:                    fmt.Sprintf("json inválido no envelope: %v", err),
			Stage:                    stage,
			InboundValidatorEnvelope: model.InboundValidatorEnvelope{},
			Metadata:                 model.OutboundValidatorMetadata{},
		}, msg.Key)
		return 0, nil
	}

	stage = "schema_validation"
	schema, err := p.reg.LoadSchema(env.DeviceType, env.SchemaVersion, env.EventType)
	if err != nil {
		p.emitDLQ(ctx, model.OutboundValidatorDLQEnvelope{
			Error:                    fmt.Sprintf("schema não encontrado/carregado do redis: %v", err),
			Stage:                    stage,
			InboundValidatorEnvelope: env,
			Metadata:                 p.buildMetadata(&env),
		}, []byte(env.DeviceID))
		return 0, nil
	}

	metadata := p.buildMetadata(&env)

	var payloadObj any

	if err := json.Unmarshal(env.Payload, &payloadObj); err != nil {
		p.emitDLQ(ctx, model.OutboundValidatorDLQEnvelope{
			Error:                    fmt.Sprintf("payload não é JSON válido: %v", err),
			Stage:                    stage,
			InboundValidatorEnvelope: env,
			Metadata:                 metadata,
		}, []byte(env.DeviceID))
		return 0, nil
	}

	if err := schema.Validate(payloadObj); err != nil {
		p.emitDLQ(ctx, model.OutboundValidatorDLQEnvelope{
			Error:                    fmt.Sprintf("falha na validação do payload: %v", err),
			Stage:                    stage,
			InboundValidatorEnvelope: env,
			Metadata:                 metadata,
		}, []byte(env.DeviceID))
		return 0, nil
	}

	out := model.OutboundValidatorEnvelope{InboundValidatorEnvelope: env, Metadata: metadata}

	outBytes, err := json.Marshal(out)

	if err != nil {
		p.emitDLQ(ctx, model.OutboundValidatorDLQEnvelope{
			Error:                    fmt.Sprintf("falha ao serializar validated: %v", err),
			Stage:                    "marshal_validated",
			InboundValidatorEnvelope: env,
			Metadata:                 metadata,
		}, []byte(env.DeviceID))
		return 0, nil
	}

	if err := p.outWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(env.DeviceID),
		Value: outBytes,
	}); err != nil {
		p.emitDLQ(ctx, model.OutboundValidatorDLQEnvelope{
			Error:                    fmt.Sprintf("falha ao publicar validated: %v", err),
			Stage:                    "publish_validated",
			InboundValidatorEnvelope: env,
			Metadata:                 metadata,
		}, []byte(env.DeviceID))
		return 0, nil
	}

	return time.Since(start), nil
}

func (p *Processor) emitDLQ(ctx context.Context, evt model.OutboundValidatorDLQEnvelope, key []byte) {
	if evt.Metadata.EventID == "" {
		evt.Metadata = p.buildMetadata(&evt.InboundValidatorEnvelope)
	}

	b, _ := json.Marshal(evt)

	_ = p.dlqWriter.WriteMessages(ctx, kafka.Message{Key: key, Value: b})
}

func (p *Processor) buildMetadata(env *model.InboundValidatorEnvelope) model.OutboundValidatorMetadata {
	now := time.Now().UTC()

	return model.OutboundValidatorMetadata{
		EventID:     uuid.NewString(),
		CollectedAt: env.InboundMetadata.CollectedAt,
		ValidatedAt: now,
	}
}
