package relay

import (
	"os"

	log "github.com/golang/glog"

	"encoding/json"

	"github.com/streadway/amqp"
)

type M map[string]interface{}

func flattenMeteringData(meteringData map[string]map[string]int) []map[string]interface{} {

	meteringArray := make([]map[string]interface{}, 0)
	gockyId, err := os.Hostname()
	if err != nil {
		gockyId = ""
	}

	mu.RLock()
	defer mu.RUnlock()
	for orgId, values := range meteringData {
		for machineId, counter := range values {
			m := M{"gockyId": gockyId, "owner": orgId, "machine": machineId, "counter": counter}
			meteringArray = append(meteringArray, m)
		}
	}

	return meteringArray
}

func pushToAmqp() {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalln(err)
	}
	defer ch.Close()

	body, err := json.Marshal(flattenMeteringData(metering))
	if err != nil {
		return
	}

	q, err := ch.QueueDeclare(
		"metering", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)

	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})

	if err != nil {
		log.Errorf("Error while publishing: %s", err)
	}

}
