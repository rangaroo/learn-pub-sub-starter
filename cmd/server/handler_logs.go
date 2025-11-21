package main
import (
	"fmt"

	"github.com/rangaroo/learn-pub-sub-starter/internal/routing"
	"github.com/rangaroo/learn-pub-sub-starter/internal/gamelogic"
	"github.com/rangaroo/learn-pub-sub-starter/internal/pubsub"
)

func handlerLogs() func(gamelog routing.GameLog) pubsub.Acktype {
    return func(gamelog routing.GameLog) pubsub.Acktype {
        defer fmt.Println("> ")
        err := gamelogic.WriteLog(gamelog)
        if err != nil {
            return pubsub.NackRequeue
        }
        return pubsub.Ack
    }
}
