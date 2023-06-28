package main

import (
	"log"
	"os"
	"tp1/common/middleware"
)

func main() {
	instanceID := os.Getenv("ID")

	consumer, err := middleware.NewConsumer("consumer", "")
	if err != nil {
		log.Panic(err)
	}
	producer, err := middleware.NewProducer("producer")
	if err != nil {
		log.Panic(err)
	}
	distanceAverager := NewDistanceAverager(instanceID, consumer, producer)
	err = distanceAverager.Run()
	if err != nil {
		log.Panic(err)
	}
}
