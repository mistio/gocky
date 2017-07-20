package relay

import (
	"encoding/hex"

	"golang.org/x/crypto/scrypt"
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
	salt := []byte("asdfasdf")

	fieldByte := []byte(p.Field)
	bytestring := append(key, fieldByte...)

	hash, _ := scrypt.Key(bytestring, salt, 16384, 8, 1, 32)
	p.ID = hex.EncodeToString(hash)
	// log.Println(string(p.ID))
	// log.Println(p.Value)
}
