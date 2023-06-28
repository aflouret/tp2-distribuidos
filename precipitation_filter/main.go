package main

import (
	"log"
	"os"
	"strconv"
	"tp1/common/middleware"
)

func main() {
	minimumPrecipitations, err := strconv.ParseFloat(os.Getenv("MIN_PRECIPITATIONS"), 64)
	if err != nil {
		minimumPrecipitations = 30
	}

	consumer, err := middleware.NewConsumer("consumer", "")
	if err != nil {
		log.Panic(err)
	}
	producer, err := middleware.NewProducer("producer")
	if err != nil {
		log.Panic(err)
	}
	precipitationFilter := NewPrecipitationFilter(consumer, producer, minimumPrecipitations)
	err = precipitationFilter.Run()
	if err != nil {
		log.Panic(err)
	}
}
