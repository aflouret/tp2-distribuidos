package main

import (
	log "github.com/sirupsen/logrus"
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

	consumer, err := middleware.NewConsumer("consumer", "stations")
	if err != nil {
		log.Panic(err)
	}

	yearFilterProducer, err := middleware.NewProducer("year_filter_producer")
	if err != nil {
		log.Panic(err)
	}

	distanceCalculatorProducer, err := middleware.NewProducer("distance_calculator_producer")
	if err != nil {
		log.Panic(err)
	}

	stationsJoiner := NewStationsJoiner(instanceID, consumer, yearFilterProducer, distanceCalculatorProducer)
	err = stationsJoiner.Run()
	if err != nil {
		log.Panic(err)
	}
	replier.Stop()
}
