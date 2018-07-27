package relay

import (
  "log"

  "encoding/json"
  "github.com/streadway/amqp"
)

type M map[string]interface{}

func flattenMeteringData(meteringData map[string]map[string]int) []map[string]interface{} {

  meteringArray := make([]map[string]interface{}, 0)

  mu.RLock()
  defer mu.RUnlock()

  for orgId, values := range meteringData {
    for machineId, counter := range values {
      m := M{"owner": orgId, "machine": machineId, "counter": counter}
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
    false,   // durable
    false,   // delete when unused
    false,   // exclusive
    false,   // no-wait
    nil,     // arguments
  )

  err = ch.Publish(
    "",     // exchange
    q.Name, // routing key
    false,  // mandatory
    false,  // immediate
    amqp.Publishing {
      ContentType: "text/plain",
      Body:        body,
    })

  if err != nil {
    log.Println("Error while publishing:")
    log.Println(err)
  } else {
      mu.Lock()

      for k := range metering {
        delete(metering, k)
      }

      mu.Unlock()
  }

}
