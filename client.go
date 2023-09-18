package main

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	Start()
}

func Start() {
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

	// External function that listens to the queue and processes messages
	for {
		msg, err := Dequeue(msgs)
		if err != nil {
			log.Printf("Error dequeuing message: %v", err)
			continue
		}

		// Execute the function that takes 2 minutes
		fmt.Println("Executing function that takes 2 minutes...", string(msg.Body))
		time.Sleep(2 * time.Second)
		fmt.Println("Function execution completed.", string(msg.Body))

		// Acknowledge the message manually
		err = msg.Ack(false)
		if err != nil {
			log.Printf("Failed to acknowledge message: %v", err)
		}
	}
}

// Dequeue function to retrieve a message from the channel
func Dequeue(msgs <-chan amqp.Delivery) (amqp.Delivery, error) {
	select {
	case msg, ok := <-msgs:
		if ok {
			return msg, nil
		}
		return amqp.Delivery{}, fmt.Errorf("Channel closed")
	}
}
