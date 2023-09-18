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

	// Declare the same queue as the server
	queueName := "my_queue"

	// Consume messages from the queue with manual acknowledgment
	msgs, err := ch.Consume(
		queueName, // Queue name
		"",        // Consumer name (empty string generates a unique name)
		false,     // Auto-acknowledge (set to false for manual ack)
		false,     // Exclusive
		false,     // No-local
		false,     // No-wait
		nil,       // Arguments
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	// Handle incoming messages
	for msg := range msgs {
		// Simulate message processing
		fmt.Printf("Received: %s\n", msg.Body)

		// Acknowledge the message manually
		err := msg.Ack(false)
		if err != nil {
			log.Printf("Failed to acknowledge message: %v", err)
		}
	}
}
