package model

import (
	"encoding/json"
	"time"
)

type InboundEnvelope struct {
	DeviceID      string           `json:"deviceId"`
	DeviceType    string           `json:"deviceType"`
	EventType     string           `json:"eventType"`
	SchemaVersion string           `json:"schemaVersion"`
	Timestamp     *time.Time       `json:"timestamp"`
	Payload       json.RawMessage  `json:"payload"`
	Metadata      *InboundMetadata `json:"metadata"`
}

type InboundMetadata struct {
	EventID     string     `json:"eventId"`
	CollectedAt *time.Time `json:"collectedAt"`
	ValidatedAt *time.Time `json:"validatedAt"`
}

type OutboundEnvelope struct {
	InboundEnvelope
	Context  *ContextEnrichment `json:"context"`
	Metadata *OutboundMetadata  `json:"metadata"`
}

type OutboundMetadata struct {
	InboundMetadata
	EnrichedAt *time.Time `json:"enrichedAt"`
}

type ContextEnrichment struct {
	RegionID       string   `json:"regionId"`
	DeviceLocation string   `json:"deviceLocation"`
	DeviceCategory string   `json:"deviceCategory"`
	DeviceModel    string   `json:"deviceModel"`
	EndUserID      string   `json:"endUserId"`
	ContractType   string   `json:"contractType"`
	Programs       []string `json:"programs"`
}

type DLQEvent struct {
	InboundEnvelope
	Error    string           `json:"error"`
	Stage    string           `json:"stage"`
	Metadata OutboundMetadata `json:"metadata"`
}
