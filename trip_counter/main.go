package main

import (
	"log"
	"os"
	"tp1/common/middleware"
)

func main() {
	year := os.Getenv("YEAR")
	if year == "" {
		log.Fatal("Year not defined")
	}

	consumer, err := middleware.NewConsumer("consumer")
	if err != nil {
		log.Fatal(err)
	}
	producer, err := middleware.NewProducer("producer")
	if err != nil {
		log.Fatal(err)
	}
	tripCounter := NewTripCounter(year, consumer, producer)
	tripCounter.Run()
}
