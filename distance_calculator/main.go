package main

import (
	"log"
	"tp1/common/checkreplier"
	"tp1/common/middleware"
)

func main() {

	replier := checkreplier.NewReplier()
	if err := replier.Run(); err != nil {
		log.Fatal(err)
	}

	consumer, err := middleware.NewConsumer("consumer", "")
	if err != nil {
		log.Fatal(err)
	}
	producer, err := middleware.NewProducer("producer")
	if err != nil {
		log.Fatal(err)
	}
	distanceCalculator := NewDistanceCalculator(producer, consumer)
	distanceCalculator.Run()

	replier.Stop()
}
