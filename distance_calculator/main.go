package main

import (
	"log"
	"tp1/common/middleware"
)

func main() {
	consumer, err := middleware.NewConsumer("consumer", "")
	if err != nil {
		log.Panic(err)
	}
	producer, err := middleware.NewProducer("producer")
	if err != nil {
		log.Panic(err)
	}
	distanceCalculator := NewDistanceCalculator(producer, consumer)
	err = distanceCalculator.Run()
	if err != nil {
		log.Panic(err)
	}
}
