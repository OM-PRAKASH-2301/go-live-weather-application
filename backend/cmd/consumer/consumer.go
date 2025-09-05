package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "weather-updates",
		GroupID: "weather-consumer-group",
	})
	defer reader.Close()

	log.Println("Kafka Consumer started. Listening for messages...")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("Error reading message:", err)
			break
		}
		log.Printf("Received message: key=%s value=%s\n", string(msg.Key), string(msg.Value))
	}
}
