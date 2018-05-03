package relay

import (
  "log"

  "encoding/json"
  "github.com/streadway/amqp"
)

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

  body, err := json.Marshal(metering)
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
  }

}
