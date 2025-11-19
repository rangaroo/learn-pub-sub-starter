package main
import (
	"fmt"
	"github.com/rangaroo/learn-pub-sub-starter/internal/routing"
	"github.com/rangaroo/learn-pub-sub-starter/internal/gamelogic"
	"github.com/rangaroo/learn-pub-sub-starter/internal/pubsub"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Println("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState) func(mv gamelogic.ArmyMove) pubsub.Acktype {
	return func(mv gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Println("> ")
		switch gs.HandleMove(mv) {
		case gamelogic.MoveOutComeSafe, gamelogic.MoveOutcomeMakeWar:
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}
