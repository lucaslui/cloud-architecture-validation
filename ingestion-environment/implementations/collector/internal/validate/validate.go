package validate

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/lucaslui/hems/collector/internal/model"
)

func ValidatePayload(raw []byte) (model.InboundEnvelope, error) {
	var e model.InboundEnvelope
	if err := json.Unmarshal(raw, &e); err != nil {
		return model.InboundEnvelope{}, err
	}
	if strings.TrimSpace(e.DeviceID) == "" {
		return model.InboundEnvelope{}, errors.New("missing field: deviceId")
	}
	if strings.TrimSpace(e.DeviceType) == "" {
		return model.InboundEnvelope{}, errors.New("missing field: deviceType")
	}
	if strings.TrimSpace(e.EventType) == "" {
		return model.InboundEnvelope{}, errors.New("missing field: eventType")
	}
	if strings.TrimSpace(e.SchemaVersion) == "" {
		return model.InboundEnvelope{}, errors.New("missing field: schemaVersion")
	}
	if e.Payload == nil {
		return model.InboundEnvelope{}, errors.New("missing field: payload")
	}
	if e.Timestamp == nil {
		return model.InboundEnvelope{}, errors.New("missing field: timestamp")
	}
	return e, nil
}

func Truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}
