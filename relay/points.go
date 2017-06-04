package relay

import (
	"fmt"
	"strconv"

	"github.com/influxdata/influxdb/models"
)

// TransformedPoint is an interface to transform models.ParsePointsWithPrecision
// to desired output format
type TransformedPoint interface {
	// Transform will transform
	Transform() error
}

// InfluxToBeringeiPoint is a formatted Influxdb to BeringeiPoint
type InfluxToBeringeiPoint struct {
	point models.Point
	// tags  []models.Tag
	// tags map[string]string

	metricName  string
	machineID   string
	beringeiKey string
	fields      map[string]string
	Timestamp   int64
	Output      string
}

// Transform is an init/New equivalent
func (p *InfluxToBeringeiPoint) Transform() error {

	p.metricName = string(p.point.Name())
	p.Timestamp = p.point.UnixNano()

	p.findMachineID()

	if p.machineID != "" {
		p.beringeiKey += p.machineID + "."
	}
	p.beringeiKey += p.metricName

	p.toFields()

	for k, v := range p.fields {
		newOutput := fmt.Sprintf("%s.%s %s %d\n", p.beringeiKey, k, v, p.Timestamp)
		p.Output += newOutput

	}
	return nil
}

func (p *InfluxToBeringeiPoint) findMachineID() error {
	for _, tag := range p.point.Tags() {
		if string(tag.Key) == "machine_id" {
			p.machineID = string(tag.Value)
		}
	}
	return nil
}

func (p *InfluxToBeringeiPoint) toFields() error {
	if p.fields == nil {
		p.fields = make(map[string]string)
	}

	fi := p.point.FieldIterator()
	for fi.Next() {
		switch fi.Type() {
		case models.Float:
			v, _ := fi.FloatValue()
			p.fields[string(fi.FieldKey())] = strconv.FormatFloat(v, 'E', -1, 64)
		case models.Integer:
			v, _ := fi.IntegerValue()
			p.fields[string(fi.FieldKey())] = strconv.FormatInt(v, 10)
		case models.String:
			v := fi.StringValue()
			p.fields[string(fi.FieldKey())] = v
		case models.Boolean:
			v, _ := fi.BooleanValue()
			p.fields[string(fi.FieldKey())] = strconv.FormatBool(v)
		case models.Empty:
			p.fields[string(fi.FieldKey())] = ""
		default:
			panic("unknown type")
		}
	}
	return nil
}
