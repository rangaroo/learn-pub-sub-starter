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

	<-make(chan struct{})
}
