package pubsub
import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"encoding/json"
)

type SimpleQueueType string
const (
	QueueDurable   SimpleQueueType = "durable"
	QueueTransient SimpleQueueType = "transient"
)

type Acktype int
const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
    conn *amqp.Connection,
    exchange,
    queueName,
    key string,
    queueType SimpleQueueType, // an enum to represent "durable" or "transient"
    handler func(T) Acktype,
) error {
	c, q, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could't declare and bind queue: %w", err)
	}

	d, err := c.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could't consume messages: %w", err)
	}

	go func() {
		for msg := range d {
			var dat T
			err := json.Unmarshal(msg.Body, &dat)
			if err != nil {
				fmt.Errorf("could't unmarshal message: %w", err)
			}

			switch handler(dat) {
			case Ack:
				msg.Ack(false)
				fmt.Println("Ack")
			case NackDiscard:
				msg.Nack(false, false)
				fmt.Println("NackDiscard")
			case NackRequeue:
				msg.Nack(false, true)
				fmt.Println("NackRequeue")
			}
		}
	}()

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could't create channel: %w", err)
	}

	q, err := channel.QueueDeclare(
		queueName,
		queueType == QueueDurable,
		queueType == QueueTransient,
		queueType == QueueTransient,
		false,
		amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
		},
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could't create new queue: %w", err)
	}

	err = channel.QueueBind(q.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could't bind queue: %w", err)
	}

	return channel, q, nil
}
