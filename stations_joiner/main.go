package main

import (
	log "github.com/sirupsen/logrus"
	"tp1/common/middleware"
)

func main() {
	stationsConsumer, err := middleware.NewConsumer("stations_consumer")
	if err != nil {
		log.Fatal(err)
	}

	tripsConsumer, err := middleware.NewConsumer("trips_consumer")
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

	stationsJoiner := NewStationsJoiner(tripsConsumer, stationsConsumer, yearFilterProducer, distanceCalculatorProducer)
	stationsJoiner.Run()
}
