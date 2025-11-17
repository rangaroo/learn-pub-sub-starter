package main

import (
	"log"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rangaroo/learn-pub-sub-starter/internal/routing"
	"github.com/rangaroo/learn-pub-sub-starter/internal/pubsub"
)

func main() {
	const CONNECTION_STRING = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(CONNECTION_STRING)
	if err != nil {
		log.Fatalf("Could't create connection to RabbitMQ", err)
	}
	defer conn.Close()
	log.Println("Successfully connected to RabbitMQ")

	channel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Could't create channel for the connection")
	}

	err = pubsub.PublishJSON(
		channel, 
		routing.ExchangePerilDirect, 
		routing.PauseKey, 
		routing.PlayingState{ 
			IsPaused: true, 
		},
	)
	if err != nil {
		log.Fatalf("Error: %w", err)
	}
	log.Println("Pause message sent!")
}
