package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
)

func main() {
	//getting cluster, broker and topic informations
	viper.AddConfigPath("../config")
	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.ReadInConfig()
	server := viper.Get("bootstrapServer")
	topic := viper.Get("topicName")
	ctx := context.Background()

	produce(ctx, server.(string), topic.(string))
}

func produce(ctx context.Context, server string, topic string) {
	i := 0

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{server},
		Topic:   topic,
	})

	fmt.Println("Welcome! You can start producing your messages.")
	fmt.Println("---------------------")
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("-> ")
		text, _ := reader.ReadString('\n')
		err := w.WriteMessages(ctx, kafka.Message{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(text),
		})
		if err != nil {
			panic("could not write message " + err.Error())
		}
	}
}
