package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type WeatherData struct {
	Location    string  `json:"location"`
	Temperature float64 `json:"temperature"`
	Humidity    int     `json:"humidity"`
	Timestamp   string  `json:"timestamp"`
}

func main() {
	// Kafka writer configuration
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "weather-updates",
	})
	defer writer.Close()

	log.Println("Kafka Producer started. Sending weather data every 5 seconds...")

	for {
		data := WeatherData{
			Location:    "New York",
			Temperature: 22.5,
			Humidity:    60,
			Timestamp:   time.Now().Format(time.RFC3339),
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			log.Println("Error marshaling data:", err)
			continue
		}

		err = writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(data.Location),
				Value: jsonData,
			},
		)
		if err != nil {
			log.Println("Error writing message:", err)
		} else {
			log.Printf("Sent weather data: %+v\n", data)
		}

		time.Sleep(1 * time.Second)
	}
}
