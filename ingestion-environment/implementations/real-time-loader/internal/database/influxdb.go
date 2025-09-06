package database

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"

	"github.com/lucaslui/hems/real-time-loader/internal/config"
	"github.com/lucaslui/hems/real-time-loader/internal/model"
)

type InfluxDB struct {
	Client              influxdb2.Client
	WriteAPI            api.WriteAPIBlocking
	MeasurementTemplate string
}

func NewInfluxDB(cfg *config.Config) *InfluxDB {
	client := influxdb2.NewClient(cfg.InfluxURL, cfg.InfluxToken)
	writeAPI := client.WriteAPIBlocking(cfg.InfluxOrg, cfg.InfluxBucket)
	return &InfluxDB{
		Client:              client,
		WriteAPI:            writeAPI,
		MeasurementTemplate: cfg.InfluxMeasurementTemplate,
	}
}

func (db *InfluxDB) Close() {
	if db != nil && db.Client != nil {
		db.Client.Close()
	}
}

func (db *InfluxDB) WriteEvent(ctx context.Context, evt *model.EnrichedEvent) error {
	measurement := db.resolveMeasurement(evt.DeviceType)
	point := db.buildPoint(measurement, evt)
	return db.WriteAPI.WritePoint(ctx, point)
}

// ---------- helpers ----------

func (db *InfluxDB) resolveMeasurement(deviceType string) string {
	name := sanitizeName(strings.ToLower(deviceType))
	tmpl := db.MeasurementTemplate
	if tmpl == "" {
		tmpl = "%s"
	}
	return fmt.Sprintf(tmpl, name)
}

var invalidRe = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

func sanitizeName(s string) string {
	s = strings.TrimSpace(s)
	s = invalidRe.ReplaceAllString(s, "_")
	s = strings.Trim(s, "_")
	if s == "" {
		return "unknown"
	}
	return s
}

func (db *InfluxDB) buildPoint(measurement string, evt *model.EnrichedEvent) *write.Point {
	// Tags (atenção à cardinalidade em produção)
	tags := map[string]string{
		"deviceId":       evt.DeviceID,
		"deviceType":     evt.DeviceType,
		"eventType":      evt.EventType,
		"schemaVersion":  evt.SchemaVersion,
		"regionId":       evt.Metadata.Enrichment.RegionID,
		"deviceLocation": evt.Metadata.Enrichment.DeviceLocation,
		"deviceCategory": evt.Metadata.Enrichment.DeviceCategory,
		"deviceModel":    evt.Metadata.Enrichment.DeviceModel,
		"endUserId":      evt.Metadata.Enrichment.EndUserID,
		"contractType":   evt.Metadata.Enrichment.ContractType,
	}

	// Fields dinâmicos a partir do payload (flatten)
	fields := make(map[string]interface{})
	flat := make(map[string]interface{})
	flatten("", evt.Payload, flat)
	for k, v := range flat {
		if fv, ok := normalizeFieldValue(v); ok {
			fields[sanitizeFieldKey(k)] = fv
		}
	}
	// opcional: programas como string única
	if len(evt.Metadata.Enrichment.Programs) > 0 {
		fields["programs"] = strings.Join(evt.Metadata.Enrichment.Programs, ",")
	}

	ts := evt.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	return write.NewPoint(measurement, tags, fields, ts)
}

// flatten: obj/array -> chaves separadas por "_"
func flatten(prefix string, v interface{}, out map[string]interface{}) {
	key := func(k string) string {
		if prefix == "" {
			return k
		}
		return prefix + "_" + k
	}
	switch t := v.(type) {
	case map[string]interface{}:
		for k, val := range t {
			flatten(key(k), val, out)
		}
	case []interface{}:
		parts := make([]string, 0, len(t))
		for _, item := range t {
			parts = append(parts, toScalarString(item))
		}
		out[prefix] = strings.Join(parts, ",")
	default:
		out[prefix] = t
	}
}

func toScalarString(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64, float32, int, int64, uint64, int32, uint32, int16, uint16, int8, uint8:
		return fmt.Sprintf("%v", x)
	case map[string]interface{}:
		tmp := make(map[string]interface{})
		flatten("", x, tmp)
		parts := make([]string, 0, len(tmp))
		for k, v := range tmp {
			parts = append(parts, k+":"+fmt.Sprintf("%v", v))
		}
		return strings.Join(parts, "|")
	default:
		return fmt.Sprintf("%v", x)
	}
}

func normalizeFieldValue(v interface{}) (interface{}, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint64:
		return float64(x), true
	case int32:
		return float64(x), true
	case uint32:
		return float64(x), true
	case int16:
		return float64(x), true
	case uint16:
		return float64(x), true
	case int8:
		return float64(x), true
	case uint8:
		return float64(x), true
	case bool:
		return x, true
	case string:
		return x, true
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano), true
	default:
		return nil, false
	}
}

var fieldKeyRe = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

func sanitizeFieldKey(k string) string {
	k = strings.TrimSpace(k)
	k = strings.ReplaceAll(k, " ", "_")
	k = fieldKeyRe.ReplaceAllString(k, "_")
	k = strings.Trim(k, "_")
	if k == "" {
		return "field"
	}
	return k
}
