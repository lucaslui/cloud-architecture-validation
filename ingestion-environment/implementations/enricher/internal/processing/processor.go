package processing

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lucaslui/hems/enricher-validator/internal/config"
	"github.com/lucaslui/hems/enricher-validator/internal/data"
	"github.com/lucaslui/hems/enricher-validator/internal/model"
)

type Processor struct {
	cfg       config.Config
	ctxStore  *data.Store
	inTopic   string
	outWriter *kafka.Writer
	dlqWriter *kafka.Writer
}

func NewProcessor(cfg config.Config, store *data.Store, out, dlq *kafka.Writer) *Processor {
	return &Processor{
		cfg: cfg, ctxStore: store,
		inTopic: cfg.InputTopic, outWriter: out, dlqWriter: dlq,
	}
}

func (p *Processor) Process(ctx context.Context, msg kafka.Message) (latency time.Duration, err error) {
	start := time.Now()
	stage := "decode_envelope"

	var env model.InboundEnvelope

	if err := json.Unmarshal(msg.Value, &env); err != nil {
		p.emitDLQ(ctx, model.DLQEvent{
			Error:           fmt.Sprintf("json inválido no envelope: %v", err),
			Stage:           stage,
			InboundEnvelope: model.InboundEnvelope{},
			Metadata:        p.buildMetadata(nil, nil),
		}, msg.Key)
		return 0, nil
	}

	stage = "context_enrichment"

	var enrichment *model.ContextEnrichment

	if p.ctxStore != nil {
		enrichment = p.ctxStore.Get()
	}

	metadata := p.buildMetadata(&env, enrichment)

	out := model.OutboundEnvelope{
		InboundEnvelope: env,
		Context:         enrichment,
		Metadata:        &metadata,
	}

	outBytes, err := json.Marshal(out)

	if err != nil {
		p.emitDLQ(ctx, model.DLQEvent{
			Error:           fmt.Sprintf("falha ao serializar enriched: %v", err),
			Stage:           "marshal_enriched",
			InboundEnvelope: env,
			Metadata:        metadata,
		}, []byte(env.DeviceID))
		return 0, nil
	}

	if err := p.outWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(env.DeviceID),
		Value: outBytes,
	}); err != nil {
		p.emitDLQ(ctx, model.DLQEvent{
			Error:           fmt.Sprintf("falha ao publicar enriched: %v", err),
			Stage:           "publish_enriched",
			InboundEnvelope: env,
			Metadata:        metadata,
		}, []byte(env.DeviceID))
		return 0, nil
	}

	return time.Since(start), nil
}

func (p *Processor) emitDLQ(ctx context.Context, evt model.DLQEvent, key []byte) {
	if evt.Metadata.EventID == "" {
		evt.Metadata = p.buildMetadata(&evt.InboundEnvelope, nil)
	}

	b, _ := json.Marshal(evt)

	_ = p.dlqWriter.WriteMessages(ctx, kafka.Message{Key: key, Value: b})
}

func (p *Processor) buildMetadata(env *model.InboundEnvelope, enrichment *model.ContextEnrichment) model.OutboundMetadata {
	now := time.Now().UTC()

	return model.OutboundMetadata{
		InboundMetadata: model.InboundMetadata{
			EventID:     env.Metadata.EventID,
			CollectedAt: env.Metadata.CollectedAt,
			ValidatedAt: env.Metadata.ValidatedAt,
		},
		EnrichedAt: &now,
	}
}
