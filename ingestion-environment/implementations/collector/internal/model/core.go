package model

import "time"

type InboundCollectorEnvelope struct {
	DeviceID      string         `json:"deviceId"`
	DeviceType    string         `json:"deviceType"`
	EventType     string         `json:"eventType"`
	SchemaVersion string         `json:"schemaVersion"`
	Timestamp     *time.Time     `json:"timestamp"`
	Payload       map[string]any `json:"payload"`
}

type OutboundCollectorEnvelope struct {
	InboundCollectorEnvelope
	Metadata OutboundCollectorMetadata `json:"metadata"`
}

type OutboundCollectorMetadata struct {
	EventID     string    `json:"eventId"`
	CollectedAt time.Time `json:"collectedAt"`
}
