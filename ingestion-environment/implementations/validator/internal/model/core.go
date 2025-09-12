package model

import (
	"encoding/json"
	"time"
)

type InboundValidatorEnvelope struct {
	DeviceID        string                    `json:"deviceId"`
	DeviceType      string                    `json:"deviceType"`
	EventType       string                    `json:"eventType"`
	SchemaVersion   string                    `json:"schemaVersion"`
	Timestamp       *time.Time                `json:"timestamp"`
	Payload         json.RawMessage           `json:"payload"`
	InboundMetadata *InboundValidatorMetadata `json:"metadata"`
}

type InboundValidatorMetadata struct {
	EventID     string    `json:"eventId"`
	CollectedAt time.Time `json:"collectedAt"`
}

type OutboundValidatorEnvelope struct {
	InboundValidatorEnvelope
	Metadata OutboundValidatorMetadata `json:"metadata"`
}

type OutboundValidatorMetadata struct {
	EventID     string    `json:"eventId"`
	CollectedAt time.Time `json:"collectedAt"`
	ValidatedAt time.Time `json:"validatedAt"`
}

type OutboundValidatorDLQEnvelope struct {
	InboundValidatorEnvelope
	Error    string                    `json:"error"`
	Stage    string                    `json:"stage"`
	Metadata OutboundValidatorMetadata `json:"metadata"`
}
