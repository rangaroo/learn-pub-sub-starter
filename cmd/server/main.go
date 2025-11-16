package main

import (
	"log"
	"os"
	"os/signal"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const CONNECTION_STRING = "amqp://guest:guest@localhost:5672/"
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	conn, err := amqp.Dial(CONNECTION_STRING)
	if err != nil {
		log.Fatalf("Could't create connection to RabbitMQ", err)
	}

	log.Println("Successfully connection to RabbitMQ")

	<-signalChan
	log.Println("Exiting the program...")
	defer conn.Close()
}
