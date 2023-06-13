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
	weatherConsumer, err := middleware.NewConsumer("weather_consumer")
	if err != nil {
		log.Fatal(err)
	}
	tripsConsumer, err := middleware.NewConsumer("trips_consumer")
	if err != nil {
		log.Fatal(err)
	}

	weatherJoiner := NewWeatherJoiner(producer, weatherConsumer, tripsConsumer)
	weatherJoiner.Run()
}
