package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
)

func main() {
	viper.AddConfigPath("../config")
	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.ReadInConfig()
	server := viper.Get("bootstrapServer")
	topic := viper.Get("topicName")
	ctx := context.Background()

	consume(ctx, server.(string), topic.(string), "first-group")
}

func consume(ctx context.Context, server string, topic string, groupid string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{server},
		Topic:       topic,
		GroupID:     groupid,
		StartOffset: kafka.LastOffset,
	})

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}
		fmt.Println("received: ", string(msg.Value))
	}
}
