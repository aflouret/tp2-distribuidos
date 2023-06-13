package main

import (
	"log"
	"os"
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

	consumer, err := middleware.NewConsumer("consumer")
	if err != nil {
		log.Fatal(err)
	}
	producer, err := middleware.NewProducer("producer")
	if err != nil {
		log.Fatal(err)
	}
	yearFilter := NewYearFilter(producer, consumer, year1, year2)
	yearFilter.Run()
}
