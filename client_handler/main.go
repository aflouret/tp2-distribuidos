package main

import (
	"log"
	"tp1/common/middleware"
)

func main() {
	resultsConsumer, err := middleware.NewConsumer("results_consumer")
	if err != nil {
		log.Fatal(err)
	}
	tripsProducer, err := middleware.NewProducer("trips_producer")
	if err != nil {
		log.Fatal(err)
	}
	stationsProducer, err := middleware.NewProducer("stations_producer")
	if err != nil {
		log.Fatal(err)
	}
	weatherProducer, err := middleware.NewProducer("weather_producer")
	if err != nil {
		log.Fatal(err)
	}

	clientHandler := NewClientHandler(tripsProducer, stationsProducer, weatherProducer, resultsConsumer)
	clientHandler.Run()
}
