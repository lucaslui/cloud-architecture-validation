package processing

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/lucaslui/hems/enricher-validator/internal/config"
	"github.com/lucaslui/hems/enricher-validator/internal/data"
	"github.com/lucaslui/hems/enricher-validator/internal/model"
	"github.com/lucaslui/hems/enricher-validator/internal/registry"
)

type Processor struct {
	cfg       config.Config
	reg       registry.SchemaRegistry
	ctxStore  *data.Store
	inTopic   string
	outWriter *kafka.Writer
	dlqWriter *kafka.Writer
}

func NewProcessor(cfg config.Config, reg registry.SchemaRegistry, store *data.Store, out, dlq *kafka.Writer) *Processor {
	return &Processor{
		cfg: cfg, reg: reg, ctxStore: store,
		inTopic: cfg.InputTopic, outWriter: out, dlqWriter: dlq,
	}
}

func (p *Processor) Process(ctx context.Context, msg kafka.Message) (latency time.Duration, err error) {
	start := time.Now()
	stage := "decode_envelope"

	var env model.Envelope

	if err := json.Unmarshal(msg.Value, &env); err != nil {
		p.emitDLQ(ctx, model.DLQEvent{
			Error:    fmt.Sprintf("json inválido no envelope: %v", err),
			Stage:    stage,
			Envelope: model.Envelope{},
			Metadata: p.buildMetadata(nil, &start, "", nil),
		}, msg.Key)
		return 0, nil
	}

	// validação
	stage = "schema_validation"
	schema, schemaRef, err := p.reg.Load(env.DeviceType, env.SchemaVersion, env.EventType)
	if err != nil {
		p.emitDLQ(ctx, model.DLQEvent{
			Error:    fmt.Sprintf("schema não encontrado/carregado do redis: %v", err),
			Stage:    stage,
			Envelope: env,
			Metadata: p.buildMetadata(&env, &start, schemaRef, nil),
		}, []byte(env.DeviceID))
		return 0, nil
	}

	var payloadObj any

	if err := json.Unmarshal(env.Payload, &payloadObj); err != nil {
		p.emitDLQ(ctx, model.DLQEvent{
			Error:    fmt.Sprintf("payload não é JSON válido: %v", err),
			Stage:    stage,
			Envelope: env,
			Metadata: p.buildMetadata(&env, &start, schemaRef, nil),
		}, []byte(env.DeviceID))
		return 0, nil
	}

	if err := schema.Validate(payloadObj); err != nil {
		p.emitDLQ(ctx, model.DLQEvent{
			Error:    fmt.Sprintf("falha na validação do payload: %v", err),
			Stage:    stage,
			Envelope: env,
			Metadata: p.buildMetadata(&env, &start, schemaRef, nil),
		}, []byte(env.DeviceID))
		return 0, nil
	}

	stage = "context_enrichment"

	var enrichment *model.Enrichment

	if p.ctxStore != nil {
		enrichment = p.ctxStore.Get()
	}

	hash := sha256Hex(env.Payload)

	metadata := p.buildMetadata(&env, &start, schemaRef, enrichment)
	metadata.Hash = hash

	out := model.EnrichedEvent{Envelope: env, Metadata: metadata}

	outBytes, err := json.Marshal(out)

	if err != nil {
		p.emitDLQ(ctx, model.DLQEvent{
			Error:    fmt.Sprintf("falha ao serializar enriched: %v", err),
			Stage:    "marshal_enriched",
			Envelope: env,
			Metadata: metadata,
		}, []byte(env.DeviceID))
		return 0, nil
	}

	if err := p.outWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(env.DeviceID),
		Value: outBytes,
	}); err != nil {
		p.emitDLQ(ctx, model.DLQEvent{
			Error:    fmt.Sprintf("falha ao publicar enriched: %v", err),
			Stage:    "publish_enriched",
			Envelope: env,
			Metadata: metadata,
		}, []byte(env.DeviceID))
		return 0, nil
	}

	return time.Since(start), nil
}

func (p *Processor) emitDLQ(ctx context.Context, evt model.DLQEvent, key []byte) {
	if evt.Metadata.EventID == "" {
		evt.Metadata = p.buildMetadata(&evt.Envelope, nil, "", nil)
	}

	b, _ := json.Marshal(evt)

	_ = p.dlqWriter.WriteMessages(ctx, kafka.Message{Key: key, Value: b})
}

func (p *Processor) buildMetadata(env *model.Envelope, processStart *time.Time, schemaRef string, enrichment *model.Enrichment) model.Metadata {
	now := time.Now().UTC()

	var recv *time.Time
	var corrID string

	if env != nil {
		if env.Timestamp != nil {
			recv = env.Timestamp
		}
		corrID = env.CorrelationID
	}

	return model.Metadata{
		EventID:       uuid.NewString(),
		ProcessedAt:   now,
		ReceivedAt:    recv,
		SchemaRef:     schemaRef,
		CorrelationID: corrID,
		Enrichment:    enrichment,
	}
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)

	return fmt.Sprintf("%x", h[:])
}
