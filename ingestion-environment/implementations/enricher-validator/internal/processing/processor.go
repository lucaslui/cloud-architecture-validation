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
	ctxstore "github.com/lucaslui/hems/enricher-validator/internal/context"
	"github.com/lucaslui/hems/enricher-validator/internal/model"
	"github.com/lucaslui/hems/enricher-validator/internal/registry"
)

const (
	kafkaMinBytes = 1_000
	kafkaMaxBytes = 10_000_000
)

type Processor struct {
	cfg       config.Config
	reg       registry.SchemaRegistry
	ctxStore  *ctxstore.Store
	inTopic   string
	outWriter *kafka.Writer
	dlqWriter *kafka.Writer
}

func NewProcessor(cfg config.Config, reg registry.SchemaRegistry, store *ctxstore.Store, out, dlq *kafka.Writer) *Processor {
	return &Processor{
		cfg: cfg, reg: reg, ctxStore: store,
		inTopic: cfg.InputTopic, outWriter: out, dlqWriter: dlq,
	}
}

func NewReader(cfg config.Config) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		GroupID:     cfg.GroupID,
		Topic:       cfg.InputTopic,
		MinBytes:    kafkaMinBytes,
		MaxBytes:    kafkaMaxBytes,
		StartOffset: kafka.FirstOffset,
	})
}

func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.Hash{},
	}
}

func (p *Processor) Process(ctx context.Context, m kafka.Message) (latency time.Duration, err error) {
	start := time.Now()
	stage := "decode_envelope"

	var env model.Envelope
	
	if err := json.Unmarshal(m.Value, &env); err != nil {
		p.emitDLQ(ctx, model.DLQEvent{
			Error:    fmt.Sprintf("json inválido no envelope: %v", err),
			Stage:    stage,
			Envelope: model.Envelope{},
			Meta:     p.buildMeta(nil, &start, "", "", "", nil),
		}, m.Key)
		return 0, nil
	}

	if !model.IsAllowedEventType(env.EventType) {
		p.emitDLQ(ctx, model.DLQEvent{
			Error:    "eventType inválido (esperado Telemetry ou State)",
			Stage:    stage,
			Envelope: env,
			Meta:     p.buildMeta(&env, &start, "", "", "", nil),
		}, []byte(env.DeviceID))
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
			Meta:     p.buildMeta(&env, &start, p.inTopic, "", "", nil),
		}, []byte(env.DeviceID))
		return 0, nil
	}

	var payloadObj any
	if err := json.Unmarshal(env.Payload, &payloadObj); err != nil {
		p.emitDLQ(ctx, model.DLQEvent{
			Error:    fmt.Sprintf("payload não é JSON válido: %v", err),
			Stage:    stage,
			Envelope: env,
			Meta:     p.buildMeta(&env, &start, p.inTopic, schemaRef, model.EnvelopeContentTypeJSON, nil),
		}, []byte(env.DeviceID))
		return 0, nil
	}
	if err := schema.Validate(payloadObj); err != nil {
		p.emitDLQ(ctx, model.DLQEvent{
			Error:    fmt.Sprintf("falha na validação do payload: %v", err),
			Stage:    stage,
			Envelope: env,
			Meta:     p.buildMeta(&env, &start, p.inTopic, schemaRef, model.EnvelopeContentTypeJSON, nil),
		}, []byte(env.DeviceID))
		return 0, nil
	}

	// enriquecimento simples (genérico)
	stage = "enrichment"
	var enrichment *model.Enrichment
	if p.ctxStore != nil {
		enrichment = p.ctxStore.Get()
	}
	hash := sha256Hex(env.Payload)

	meta := p.buildMeta(&env, &start, p.inTopic, schemaRef, model.EnvelopeContentTypeJSON, enrichment)
	meta.Hash = hash
	out := model.EnrichedEvent{Envelope: env, Meta: meta}

	outBytes, err := json.Marshal(out)
	if err != nil {
		p.emitDLQ(ctx, model.DLQEvent{
			Error:    fmt.Sprintf("falha ao serializar enriched: %v", err),
			Stage:    "marshal_enriched",
			Envelope: env,
			Meta:     meta,
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
			Meta:     meta,
		}, []byte(env.DeviceID))
		return 0, nil
	}

	return time.Since(start), nil
}

func (p *Processor) emitDLQ(ctx context.Context, evt model.DLQEvent, key []byte) {
	if evt.Meta.EventID == "" {
		evt.Meta = p.buildMeta(&evt.Envelope, nil, p.inTopic, "", "", nil)
	}
	b, _ := json.Marshal(evt)
	_ = p.dlqWriter.WriteMessages(ctx, kafka.Message{Key: key, Value: b})
}

func (p *Processor) buildMeta(env *model.Envelope, processStart *time.Time, sourceTopic, schemaRef, contentType string, enrichment *model.Enrichment) model.Meta {
	now := time.Now().UTC()
	var recv *time.Time
	var corrID string
	if env != nil {
		if env.Timestamp != nil { recv = env.Timestamp }
		corrID = env.CorrelationID
	}
	return model.Meta{
		EventID:       uuid.NewString(),
		ProcessedAt:   now,
		ReceivedAt:    recv,
		SourceTopic:   sourceTopic,
		SchemaRef:     schemaRef,
		ContentType:   contentType,
		CorrelationID: corrID,
		Enrichment:    enrichment,
	}
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b); return fmt.Sprintf("%x", h[:])
}
