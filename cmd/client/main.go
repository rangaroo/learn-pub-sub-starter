package main

import (
	"log"
	"fmt"
	"strconv"

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

	publishChan, err := conn.Channel()
	if err != nil {
		log.Fatalf("could't create channel for the connection")
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalln("you must provide username")
	}

	gs := gamelogic.NewGameState(username)

	// Pause
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey + "." + gs.GetUsername(),
		routing.PauseKey,
		pubsub.QueueTransient,
		handlerPause(gs),
	)
	if err != nil {
		log.Fatalf("could't subscribe to pause: %v", err)
	}

	// Move
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix + "." + gs.GetUsername(),
		routing.ArmyMovesPrefix + ".*",
		pubsub.QueueTransient,
		handlerMove(gs, publishChan),
	)
	if err != nil {
		log.Fatalf("could't subscribe to army moves: %v", err)
	}

	// War
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix + ".*",
		pubsub.QueueDurable,
		handlerWar(gs, publishChan),
	)
	if err != nil {
		log.Fatalf("could't subscribe to war declarations: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			err := gs.CommandSpawn(words)
			if err != nil {
				fmt.Printf("could't spawn unit")
			}
		case "move":
			mv, err := gs.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}

			err = pubsub.PublishJSON(
				publishChan,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix + "." + mv.Player.Username,
				mv,
			)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				continue
			}

			fmt.Printf("Moved %v units to %s\n", len(mv.Units), mv.ToLocation)
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(words) < 2 {
				fmt.Println("provide second argument: 'spam <n>', e.g. 'spam 10', 'spam 1000'")
				continue
			}

			n, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Printf("could't convert %s to integer\n", words[1])
				continue
			}

			for i := 0; i < n; i++ {
				msg := gamelogic.GetMaliciousLog()
				err := publishGameLog(publishChan, gs.GetUsername(), msg)

				if err != nil {
					fmt.Printf("error: %s\n", err)
					continue
				}
			}
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("invalid command")
			continue
		}
	}
}
