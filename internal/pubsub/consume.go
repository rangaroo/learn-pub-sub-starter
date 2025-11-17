package pubsub
import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	QueueDurable   SimpleQueueType = "durable"
	QueueTransient SimpleQueueType = "transient"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could't create channel on the connection: %w", err)
	}

	q, err := channel.QueueDeclare(
		queueName,
		queueType == QueueDurable,
		queueType == QueueTransient,
		queueType == QueueTransient,
		false,
		nil,
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
