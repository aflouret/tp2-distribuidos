package main

import (
	"log"
	"os"
	"tp1/common/checkreplier"
	"tp1/common/middleware"
)

func main() {
	year1 := os.Getenv("YEAR_1")
	if year1 == "" {
		year1 = "2016"
	}

	year2 := os.Getenv("YEAR_2")
	if year2 == "" {
		year2 = "2017"
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
	yearFilter := NewYearFilter(producer, consumer, year1, year2)
	err = yearFilter.Run()
	if err != nil {
		log.Panic(err)
	}
	replier.Stop()
}
