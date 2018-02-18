package main

import (
	"log"
	"github.com/streadway/amqp"
	"path/filepath"
	"os"
	"encoding/csv"
	"strings"
)
/***
This GO file is used to read data from test.csv and send  it to Queue "GoQueue" ..
 */
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"GoQueue", // name
		true,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	filename,_ := filepath.Abs("../RabbitMQClientSample/test.csv")

	// Open CSV file
		f, err := os.Open(filename)
		if err != nil {
			panic(err)
		}
		// Read File into a Variable
		lines, err := csv.NewReader(f).ReadAll()
		if err != nil {
			panic(err)
		}
		f.Close()

		// Loop through lines & turn into object
		for i, line := range lines {
			if i > 0 {
				body := strings.Join(line,",")

				err = ch.Publish(
					"",     // exchange
					q.Name, // routing key
					false,  // mandatory
					false,  // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(body),
					})
				log.Printf(" [x] Sent %s", body)
			}
		}

	failOnError(err, "Failed to publish a message")
}
