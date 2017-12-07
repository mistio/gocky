package relay

import (
	"log"
	"regexp"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
)

// GraphiteMetric transforms a BeringeiPoint to Graphite compatible format
func GraphiteWindowsMetric(metricName string, tags map[string]string, timestamp int64, value interface{}, field string) telegraf.Metric {

	var parsedMetric map[string]interface{}

	switch metricName {
	case "cpu":
		parsedMetric, metricName = parseWindowsCPU(tags, field, metricName, value)
	case "disk":
		parsedMetric = map[string]interface{}{field: value}
		metricName = "disk." + tags["device"]
	case "diskio":
		parsedMetric = map[string]interface{}{field: value}
		metricName = "diskio." + tags["name"]
	case "net":
		parsedMetric = map[string]interface{}{field: value}
		metricName = "net." + tags["interface"]
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

func parseWindowsCPU(tags map[string]string, field, metricName string, value interface{}) (parsedMetric map[string]interface{}, metricNameFixed string) {

	metricNameFixed = "cpu_extra.total"

	// We only accept cpu-total for windows (for now)
	if tags["cpu"] != "cpu-total" {
		return nil, ""
	}

	r, _ := regexp.Compile(`usage_(.*[a-zA-Z0-9])`)
	match := r.FindStringSubmatch(field)
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
	parsedMetric = map[string]interface{}{fieldFix: value}

	switch fieldFix {
	case "idle",
		"interrupt",
		"nice",
		"softirq",
		"steal",
		"system",
		"user",
		"wait":
		return parsedMetric, metricNameFixed
	default:
		metricNameFixed = "cpu_extra"
		return parsedMetric, metricNameFixed
	}
}
