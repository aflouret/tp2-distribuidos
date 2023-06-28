package main

import (
	"log"
	"os"
	"tp1/common/checkreplier"
	"tp1/common/middleware"
)

func main() {
	instanceID := os.Getenv("ID")

	replier := checkreplier.NewReplier()
	if err := replier.Run(); err != nil {
		log.Fatal(err)
	}

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
	replier.Stop()
}
