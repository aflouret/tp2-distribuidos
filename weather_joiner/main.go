package main

import (
	"log"
	"os"
	"tp1/common/middleware"
)

func main() {
	instanceID := os.Getenv("ID")

	producer, err := middleware.NewProducer("producer")
	if err != nil {
		log.Fatal(err)
	}
	consumer, err := middleware.NewConsumer("consumer", "weather")
	if err != nil {
		log.Fatal(err)
	}

	weatherJoiner := NewWeatherJoiner(instanceID, producer, consumer)
	weatherJoiner.Run()
}
