package main

import (
	"encoding/json"
	"os"
	"os/signal"
	"sync"

	"github.com/go-redis/redis"
	"github.com/nlopes/slack"
	"go.uber.org/zap"
)

type slackPayload struct {
	slack.PostMessageParameters

	Text        string             `json:"text"`
	Attachments []slack.Attachment `json:"attachments"`
}

func killHandler(sigChan <-chan os.Signal, stopChan chan<- struct{}, sugar *zap.SugaredLogger) {
	<-sigChan
	sugar.Info("papika-san shutting down, stopping connections and goroutines")
	close(stopChan)
}

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		panic("REDIS_ADDR envar not set")
	}

	slackToken := os.Getenv("SLACK_TOKEN")
	if slackToken == "" {
		panic("SLACK_TOKEN envar not set")
	}

	logLevel := os.Getenv("LOG_LEVEL")

	cfg := zap.NewDevelopmentConfig()
	switch logLevel {
	case "DEBUG":
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "WARN":
		cfg.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "INFO":
		cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "ERROR":
		cfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}
	logger, err := cfg.Build()

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

	r := redis.NewClient(&redis.Options{Addr: redisAddr})

	client := slack.New(slackToken)
	if err != nil {
		sugar.Error("could not post message")
		return
	}
	rtm := client.NewRTM()

	done := make(chan struct{})
	sigChan := make(chan os.Signal, 1)
	go killHandler(sigChan, done, sugar)
	signal.Notify(sigChan, os.Interrupt)

	pubsub := r.PSubscribe("to_slack")

	sugar.Info("starting papika-san")
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		redisToSlack(pubsub, done, client, sugar)
		wg.Done()
	}()
	go func() {
		slackFromRedis(rtm, r, done, sugar)
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
			sugar.Debugf("received message %s", payload.Payload)

			var sPayload slackPayload
			err := json.Unmarshal([]byte(payload.Payload), &sPayload)
			if err != nil {
				sugar.Warnw("could not convert parse payload", "payload", payload, "error", err)
				_, _, err := client.PostMessage("C29133R96", slack.MsgOptionText(payload.Payload, false), slack.MsgOptionAsUser(true))
				if err != nil {
					sugar.Errorw("could not post message", "message", payload)
				}
				continue
			}

			sugar.Debugw("decoded slack payload", "payload", sPayload)
			params := slack.MsgOptionPostMessageParameters(sPayload.PostMessageParameters)
			textParam := slack.MsgOptionText(sPayload.Text, false)
			attachments := slack.MsgOptionAttachments(sPayload.Attachments...)

			_, _, err = client.PostMessage(sPayload.Channel, params, textParam, attachments)
			if err != nil {
				sugar.Errorw("could not post message", "message", payload)
			}
		}
	}
}

func slackFromRedis(rtm *slack.RTM, rclient *redis.Client, done <-chan struct{}, sugar *zap.SugaredLogger) {
	go rtm.ManageConnection()
	defer func() {
		_ = rtm.Disconnect()
	}()

	sugar.Info("starting slack listener")

	for {
		select {
		case <-done:
			sugar.Info("stopping slack listener")
			return
		case event := <-rtm.IncomingEvents:
			sugar.Debugw("received event", "event", event)
			if event.Type != "message" {
				continue
			}

			data, typeOk := event.Data.(*slack.MessageEvent)
			if !typeOk {
				sugar.Errorw("could not deserialize slack message", "event", event)
				continue
			}

			sugar.Debugw("data message", "message", data.Msg.Text)

			slackJSON, err := json.Marshal(data.Msg)
			if err != nil {
				sugar.Errorw("could not serialize slack message to JSON", "error", err)
				continue
			}
			pub := rclient.Publish("from_slack", slackJSON)
			if pub.Err() != nil {
				sugar.Errorw("could not publish to redis", "error", pub.Err())
			}
		}
	}
}
