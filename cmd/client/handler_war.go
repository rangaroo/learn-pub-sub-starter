package main
import (
	"fmt"
	"github.com/rangaroo/learn-pub-sub-starter/internal/gamelogic"
	"github.com/rangaroo/learn-pub-sub-starter/internal/pubsub"
)

func handlerWar(gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(w gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Println("> ")

        outcome, _, _ := gs.HandleWar(w)
		switch outcome {
        case gamelogic.WarOutcomeNotInvolved:
            return pubsub.NackRequeue
        case gamelogic.WarOutcomeNoUnits:
            return pubsub.NackDiscard
        case gamelogic.WarOutcomeOpponentWon:
            return pubsub.Ack
        case gamelogic.WarOutcomeYouWon:
            return pubsub.Ack
        case gamelogic.WarOutcomeDraw:
            return pubsub.Ack
        default:
            fmt.Errorf("error")
            return pubsub.NackDiscard
        }
    }
}
