package main

import (
	"os"
	"os/signal"
	"sync"

	"github.com/go-redis/redis"
	"github.com/nlopes/slack"
	"go.uber.org/zap"
)

func killHandler(sigChan <-chan os.Signal, stopChan chan<- struct{}, sugar *zap.SugaredLogger) {
	<-sigChan
	sugar.Info("papika-san shutting down, stopping connections and goroutines")
	close(stopChan)
}

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	sugar := logger.Sugar()
	defer func() {
		err := sugar.Sync()
		if err != nil {
			panic(err)
		}
	}()

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		panic("REDIS_ADDR envar not set")
	}

	slackToken := os.Getenv("SLACK_TOKEN")
	if slackToken == "" {
		panic("SLACK_TOKEN envar not set")
	}

	r := redis.NewClient(&redis.Options{Addr: redisAddr})

	client := slack.New(slackToken)
	if err != nil {
		sugar.Error("could not post message")
		return
	}

	done := make(chan struct{})
	sigChan := make(chan os.Signal, 1)
	go killHandler(sigChan, done, sugar)
	signal.Notify(sigChan, os.Interrupt)

	pubsub := r.PSubscribe("to_slack")

	sugar.Info("starting papika-san")
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		redisToSlack(pubsub, done, client, sugar)
		wg.Done()
	}()

	sugar.Info("papika-san has started")
	wg.Wait()
	sugar.Info("goodbye")
}

func redisToSlack(pubsub *redis.PubSub, done <-chan struct{}, client *slack.Client, sugar *zap.SugaredLogger) {
	c := pubsub.Channel()
	for {
		select {
		case <-done:
			return
		case payload := <-c:
			sugar.Infof("received message %s", payload.Payload)
			_, _, err := client.PostMessage("C29133R96", slack.MsgOptionText(payload.Payload, false))
			if err != nil {
				sugar.Errorw("could not post message", "message", payload)
			}
		}
	}
}
