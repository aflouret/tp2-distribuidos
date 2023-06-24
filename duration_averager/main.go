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
		log.Fatal(err)
	}
	producer, err := middleware.NewProducer("producer")
	if err != nil {
		log.Fatal(err)
	}
	durationAverager := NewDurationAverager(instanceID, consumer, producer)
	durationAverager.Run()
}
