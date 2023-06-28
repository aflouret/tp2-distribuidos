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
	replier.Stop()
}
