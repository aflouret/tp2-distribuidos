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
		log.Panic(err)
	}
	consumer, err := middleware.NewConsumer("consumer", "weather")
	if err != nil {
		log.Panic(err)
	}

	weatherJoiner := NewWeatherJoiner(instanceID, producer, consumer)
	err = weatherJoiner.Run()
	if err != nil {
		log.Panic(err)
	}
}
