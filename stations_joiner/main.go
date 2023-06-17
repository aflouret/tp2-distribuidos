package main

import (
	log "github.com/sirupsen/logrus"
	"tp1/common/middleware"
)

func main() {

	consumer, err := middleware.NewConsumer("consumer")
	if err != nil {
		log.Fatal(err)
	}

	yearFilterProducer, err := middleware.NewProducer("year_filter_producer")
	if err != nil {
		log.Fatal(err)
	}

	distanceCalculatorProducer, err := middleware.NewProducer("distance_calculator_producer")
	if err != nil {
		log.Fatal(err)
	}

	stationsJoiner := NewStationsJoiner(consumer, yearFilterProducer, distanceCalculatorProducer)
	stationsJoiner.Run()
}
