package main

import (
	"log"
	"tp1/common/middleware"
)

func main() {
	producer, err := middleware.NewProducer("producer")
	if err != nil {
		log.Fatal(err)
	}
	consumer, err := middleware.NewConsumer("consumer")
	if err != nil {
		log.Fatal(err)
	}

	weatherJoiner := NewWeatherJoiner(producer, consumer)
	weatherJoiner.Run()
}
