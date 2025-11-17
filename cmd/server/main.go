package main

import (
	"log"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rangaroo/learn-pub-sub-starter/internal/routing"
	"github.com/rangaroo/learn-pub-sub-starter/internal/gamelogic"
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

	gamelogic.PrintServerHelp()

	for true {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}
		
		if inputs[0] == "pause" {
			fmt.Println("Publishing paused game state")
			err = pubsub.PublishJSON(
				channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			if err != nil {
				log.Printf("could't publish time: %w", err)
			}
		} else if inputs[0] == "resume" {
			fmt.Println("Publishing resumes game state")
			err = pubsub.PublishJSON(
				channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				log.Printf("could't publish time: %w", err)
			}
		} else if inputs[0] == "quit" {
			log.Println("exiting the server...")
			return
		} else {
			fmt.Println("invalid command")
			continue
		}
	}
}
