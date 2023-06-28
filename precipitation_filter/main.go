package main

import (
	"log"
	"os"
	"strconv"
	"tp1/common/checkreplier"
	"tp1/common/middleware"
)

func main() {
	minimumPrecipitations, err := strconv.ParseFloat(os.Getenv("MIN_PRECIPITATIONS"), 64)
	if err != nil {
		minimumPrecipitations = 30
	}

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
	precipitationFilter := NewPrecipitationFilter(consumer, producer, minimumPrecipitations)
	precipitationFilter.Run()

	replier.Stop()
}
