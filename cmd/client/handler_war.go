package main
import (
	"fmt"
    "time"

	"github.com/rangaroo/learn-pub-sub-starter/internal/routing"
	"github.com/rangaroo/learn-pub-sub-starter/internal/gamelogic"
	"github.com/rangaroo/learn-pub-sub-starter/internal/pubsub"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerWar(gs *gamelogic.GameState, publishChan *amqp.Channel) func(dw gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(dw gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Println("> ")
        username := gs.GetUsername()
        outcome, winner, loser := gs.HandleWar(dw)
		switch outcome {
        case gamelogic.WarOutcomeNotInvolved:
            return pubsub.NackRequeue
        case gamelogic.WarOutcomeNoUnits:
            return pubsub.NackDiscard
        case gamelogic.WarOutcomeOpponentWon:
            err := publishGameLog(
                publishChan,
                username,
                fmt.Sprintf("%s won a war against %s", winner, loser),
            )
            if err != nil {
                fmt.Printf("error: %s\n", err)
                return pubsub.NackRequeue
            }
            return pubsub.Ack
        case gamelogic.WarOutcomeYouWon:
            err := publishGameLog(
                publishChan,
                username,
                fmt.Sprintf("%s won a war against %s", winner, loser),
            )
            if err != nil {
                fmt.Printf("error: %s\n", err)
                return pubsub.NackRequeue
            }
            return pubsub.Ack
        case gamelogic.WarOutcomeDraw:
            err := publishGameLog(
                publishChan,
                username,
                fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser),
            )
            if err != nil {
                fmt.Printf("error: %s\n", err)
                return pubsub.NackRequeue
            }
            return pubsub.Ack
        default:
            fmt.Errorf("error: unknown war outcome")
            return pubsub.NackDiscard
        }
    }
}

func publishGameLog(publishChan *amqp.Channel, username, msg string) error {
    return pubsub.PublishGob(
        publishChan,
        routing.ExchangePerilTopic,
        routing.GameLogSlug + "." + username,
        routing.GameLog{
            CurrentTime: time.Now().UTC(),
            Message:     msg,
            Username:    username,
        },
    )
}
