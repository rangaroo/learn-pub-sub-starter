package main

import (
	"log"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rangaroo/learn-pub-sub-starter/internal/gamelogic"
	"github.com/rangaroo/learn-pub-sub-starter/internal/pubsub"
	"github.com/rangaroo/learn-pub-sub-starter/internal/routing"
)

func main() {
	const CONNECTION_STRING = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(CONNECTION_STRING)
	if err != nil {
		log.Fatalf("could't create connection to RabbitMQ", err)
	}
	defer conn.Close()
	fmt.Println("Successfully connected to RabbitMQ")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalln("you must provide username")
	}

	_, q, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey + "." + username,
		routing.PauseKey,
		pubsub.QueueTransient,
	)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("Queue %v was created and bound\n", q.Name)

	gs := gamelogic.NewGameState(username)

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			err := gs.CommandSpawn(words)
			if err != nil {
				log.Printf("could't spawn unit")
			}
		case "move":
			_, err := gs.CommandMove(words)
			if err != nil {
				log.Println(err)
			}
			fmt.Println("Units moved successfully")
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("invalid command")
			continue
		}
	}
}
