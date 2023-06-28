package main

import (
	"log"
	"os"
	"tp1/common/checkreplier"
	"tp1/common/middleware"
)

const (
	defaultYear1 = "2016"
	defaultYear2 = "2017"
)

func main() {
	instanceID := os.Getenv("ID")

	year1 := os.Getenv("YEAR_1")
	if year1 == "" {
		year1 = defaultYear1
	}

	year2 := os.Getenv("YEAR_2")
	if year2 == "" {
		year2 = defaultYear2
	}

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
	tripCounter := NewTripCounter(instanceID, year1, year2, consumer, producer)
	err = tripCounter.Run()
	if err != nil {
		log.Panic(err)
	}
}
