package model

import "time"

type InboundAggregatedEnvelope struct {
	ControllerID        string                     `json:"controllerId"`
	ControllerTimestamp *time.Time                 `json:"controllerTimestamp"`
	Payload             []InboundCollectorEnvelope `json:"payload"`
}

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
	ControllerID        string     `json:"controllerId"`
	ControllerTimestamp *time.Time `json:"controllerTimestamp"`
	EventID             string     `json:"eventId"`
	CollectedAt         time.Time  `json:"collectedAt"`
}
