package main

import (
	"log"
	"tp1/common/checkreplier"
	"tp1/common/middleware"
)

func main() {
	consumer, err := middleware.NewConsumer("consumer", "")
	if err != nil {
		log.Panic(err)
	}
	stationsJoinerProducer, err := middleware.NewProducer("stations_joiner_producer")
	if err != nil {
		log.Panic(err)
	}
	weatherJoinerProducer, err := middleware.NewProducer("weather_joiner_producer")
	if err != nil {
		log.Panic(err)
	}

	replier := checkreplier.NewReplier()
	err = replier.Run()
	if err != nil {
		log.Fatal(err)
	}

	dataDropper := NewDataDropper(consumer, stationsJoinerProducer, weatherJoinerProducer)
	err = dataDropper.Run()
	if err != nil {
		log.Panic(err)
	}
	replier.Stop()
}
