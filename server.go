package main

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

func main() {
	// Connect to RabbitMQ server
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Create a channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	// Declare a queue
	queueName := "my_queue"
	_, err = ch.QueueDeclare(
		queueName, // Queue name
		false,     // Durable
		false,     // Delete when unused
		false,     // Exclusive
		false,     // No-wait
		nil,       // Arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	// Publish 5 messages to the queue
	for i := 1; i <= 5; i++ {
		message := fmt.Sprintf("Message %d", i)
		err := ch.Publish(
			"",        // Exchange (empty string means direct exchange)
			queueName, // Queue name
			false,     // Mandatory
			false,     // Immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(message),
			},
		)
		if err != nil {
			log.Fatalf("Failed to publish message %d: %v", i, err)
		}
		fmt.Printf("Sent: %s\n", message)
	}

	fmt.Println("Server (Producer) has sent 5 messages.")
}
