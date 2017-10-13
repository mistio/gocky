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
		parsedMetric = parseCPU(tags, field, value)
	case "disk":
		parsedMetric = map[string]interface{}{tags["device"] + "." + field: value}
	case "diskio":
		parsedMetric = map[string]interface{}{tags["name"] + "." + field: value}
	case "net":
		parsedMetric, metricName = parseNet(tags, field, metricName, value)
	case "mem":
		parsedMetric, metricName = parseMem(tags, field, metricName, value)
	case "system":
		parsedMetric, metricName = parseSystem(tags, field, metricName, value)
	case "swap":
		parsedMetric = parseSwap(tags, field, value)
	default:
		parsedMetric = map[string]interface{}{field: value}
	}

	if parsedMetric != nil {
		m1, _ := metric.New(
			metricName,
			map[string]string{"id": tags["machine_id"]},
			parsedMetric,
			time.Unix(timestamp/1000000000, 0).UTC(),
		)

		return m1
	}

	return nil
}

func parseCPU(tags map[string]string, field string, value interface{}) (parsedMetric map[string]interface{}) {

	// convert cpu0, cpu1, cpu-total -> 0, 1, total
	r, _ := regexp.Compile(`.*[cpu-](.*[a-zA-Z0-9])`)
	match := r.FindStringSubmatch(tags["cpu"])
	cpuFix := match[len(match)-1]

	// for now we drop the total subdirectory
	if cpuFix == "total" {
		return nil
	}

	r, _ = regexp.Compile(`usage_(.*[a-zA-Z0-9])`)
	match = r.FindStringSubmatch(field)
	var fieldFix string
	if len(match) > 0 {
		fieldFix = match[len(match)-1]
	} else {
		fieldFix = field
	}

	// transform fields to collectd compatible ones
	switch fieldFix {
	case "iowait":
		fieldFix = "wait"
	case "irq":
		fieldFix = "interrupt"

	}
	parsedMetric = map[string]interface{}{cpuFix + "." + fieldFix: value}

	return parsedMetric
}

func parseMem(tags map[string]string, field, metricName string, value interface{}) (parsedMetric map[string]interface{}, metricNameFixed string) {
	parsedMetric = map[string]interface{}{field: value}
	metricNameFixed = "memory"

	switch field {
	case
		"buffered",
		"cached",
		"free",
		"slab_recl",
		"slab_unrecl",
		"used":
		return parsedMetric, metricNameFixed
	default:
		metricNameFixed = "memory_extra"
		return parsedMetric, metricNameFixed

	}
}

func parseSystem(tags map[string]string, field, metricName string, value interface{}) (parsedMetric map[string]interface{}, metricNameFixed string) {
	fieldFix := field
	metricNameFixed = metricName

	const loadName = "load"

	switch field {
	case "load1":
		metricNameFixed = loadName
		fieldFix = "shortterm"
	case "load5":
		metricNameFixed = loadName
		fieldFix = "midterm"
	case "load15":
		metricNameFixed = loadName
		fieldFix = "longterm"
	}

	parsedMetric = map[string]interface{}{fieldFix: value}
	return parsedMetric, metricNameFixed
}

func parseSwap(tags map[string]string, field string, value interface{}) (parsedMetric map[string]interface{}) {
	fieldFix := field

	switch field {
	case "in":
		fieldFix = "swap_io.in"
	case "out":
		fieldFix = "swap_io.out"
	}

	parsedMetric = map[string]interface{}{fieldFix: value}
	return parsedMetric
}

func parseNet(tags map[string]string, field, metricName string, value interface{}) (parsedMetric map[string]interface{}, metricNameFixed string) {
	fieldFix := field
	metricNameFixed = "interface"

	// drop all interfaces
	if tags["interface"] == "all" {
		return nil, metricNameFixed
	}

	switch field {
	case "bytes_recv":
		fieldFix = "if_octets.rx"
	case "bytes_sent":
		fieldFix = "if_octets.tx"
	case "packets_recv":
		fieldFix = "if_packets.rx"
	case "packets_sent":
		fieldFix = "if_packets.tx"
	case "err_in":
		fieldFix = "if_errors.rx"
	case "err_out":
		fieldFix = "if_errors.tx"
	}

	parsedMetric = map[string]interface{}{tags["interface"] + "." + fieldFix: value}

	return parsedMetric, metricNameFixed
}
