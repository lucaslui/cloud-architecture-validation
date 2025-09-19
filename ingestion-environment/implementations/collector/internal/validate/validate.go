package validate

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/lucaslui/hems/collector/internal/model"
)

func ValidateAggregated(raw []byte) (model.InboundAggregatedEnvelope, error) {
	var agg model.InboundAggregatedEnvelope
	if err := json.Unmarshal(raw, &agg); err != nil {
		return model.InboundAggregatedEnvelope{}, err
	}
	if strings.TrimSpace(agg.ControllerID) == "" {
		return model.InboundAggregatedEnvelope{}, errors.New("missing field: controllerId")
	}
	if agg.ControllerTimestamp == nil {
		return model.InboundAggregatedEnvelope{}, errors.New("missing field: controllerTimestamp")
	}
	if len(agg.Payload) == 0 {
		return model.InboundAggregatedEnvelope{}, errors.New("missing/empty field: payload")
	}
	for i := range agg.Payload {
		if err := validateSingle(&agg.Payload[i]); err != nil {
			return model.InboundAggregatedEnvelope{}, errors.New("payload[" + itoa(i) + "]: " + err.Error())
		}
	}
	return agg, nil
}

func validateSingle(e *model.InboundCollectorEnvelope) error {
	if strings.TrimSpace(e.DeviceID) == "" {
		return errors.New("missing field: deviceId")
	}
	if strings.TrimSpace(e.DeviceType) == "" {
		return errors.New("missing field: deviceType")
	}
	if strings.TrimSpace(e.EventType) == "" {
		return errors.New("missing field: eventType")
	}
	if strings.TrimSpace(e.SchemaVersion) == "" {
		return errors.New("missing field: schemaVersion")
	}
	if e.Payload == nil {
		return errors.New("missing field: payload")
	}
	if e.Timestamp == nil {
		return errors.New("missing field: timestamp")
	}
	return nil
}

func Truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	const digits = "0123456789"
	var buf [20]byte
	pos := len(buf)
	for n := i; n > 0; n /= 10 {
		pos--
		buf[pos] = digits[n%10]
	}
	return string(buf[pos:])
}
