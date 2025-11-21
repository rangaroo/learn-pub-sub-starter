package pubsub
import (
    "context"
    "encoding/gob"
	"encoding/json"
	"bytes"
	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var dat bytes.Buffer
	enc := gob.NewEncoder(&dat)
	err := enc.Encode(val)
    if err != nil {
        return err
    }

    return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
        ContentType: "application/gob",
        Body:        dat.Bytes(),
	})
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
    dat, err := json.Marshal(val)
    if err != nil {
        return err
    }

    return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
        ContentType: "application/json",
        Body:        dat,
	})
}
