package relay

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
)

//BeringeiPoint is the Point that we push to Rabbitmq
type BeringeiPoint struct {
	Name      string
	Timestamp int64
	Tags      map[string]string
	Field     string
	Value     interface{}
	ID        string
}

// NewBeringeiPoint Initializes and returns a new BeringeiPoint
func NewBeringeiPoint(name, field string, timestamp int64, tags map[string]string, value interface{}) *BeringeiPoint {
	return &BeringeiPoint{
		Name:      name,
		Timestamp: timestamp,
		Tags:      tags,
		Field:     field,
		Value:     value,
	}
}

// This will take the initial telegraf key and then generate a unique Id based in the key and the Field
func (*BeringeiPoint) generateID(p *BeringeiPoint, key []byte) {

	fieldByte := []byte(p.Field)
	bytestring := append(key, fieldByte...)

	h := sha256.New()
	hash := h.Sum(bytestring)
	p.ID = hex.EncodeToString(hash)
}

// GraphiteMetric transforms a BeringeiPoint to Graphite compatible format
func GraphiteMetric(metricName string, tags map[string]string, timestamp int64, value interface{}, field string) telegraf.Metric {

	var parsedMetric map[string]interface{}

	switch metricName {
	case "cpu":
		// convert cpu0, cpu1, cpu-total -> 0, 1, total
		r, _ := regexp.Compile(`.*[cpu-](.*[a-zA-Z0-9])`)
		match := r.FindStringSubmatch(tags["cpu"])
		cpuTag := match[len(match)-1]
		parsedMetric = map[string]interface{}{cpuTag + "." + field: value}
	case "disk":
		parsedMetric = map[string]interface{}{tags["device"] + "." + field: value}
	case "diskio":
		parsedMetric = map[string]interface{}{tags["name"] + "." + field: value}
	case "net":
		parsedMetric = map[string]interface{}{tags["interface"] + "." + field: value}
	case "mem":
		parsedMetric = map[string]interface{}{"memory": value}
	default:
		parsedMetric = map[string]interface{}{field: value}
	}

	// metric.Ne
	m1, _ := metric.New(
		metricName,
		map[string]string{"id": tags["machine_id"]},
		parsedMetric,
		time.Unix(timestamp/1000000000, 0).UTC(),
	)
	return m1
}
