package pubsub
import (
    "context"
    "fmt"
    "encoding/json"
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

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
    dat, err := json.Marshal(val)
    if err != nil {
        return fmt.Errorf("could't marshal 'val': %w", err)
    }

    return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
        ContentType: "application/json",
        Body:        dat,
	})
}
