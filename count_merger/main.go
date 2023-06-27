package main

import (
	"log"
	"os"
	"tp1/common/checkreplier"
	"tp1/common/middleware"
)

const (
	defaultYear1 = "2016"
	defaultYear2 = "2017"
)

func main() {
	year1 := os.Getenv("YEAR_1")
	if year1 == "" {
		year1 = defaultYear1
	}

	year2 := os.Getenv("YEAR_2")
	if year2 == "" {
		year2 = defaultYear2
	}

	consumer, err := middleware.NewConsumer("consumer", "")
	if err != nil {
		log.Fatal(err)
	}
	producer, err := middleware.NewProducer("producer")
	if err != nil {
		log.Fatal(err)
	}

	replier := checkreplier.NewReplier()
	err = replier.Run()
	if err != nil {
		log.Fatal(err)
	}

	countMerger := NewCountMerger(consumer, producer, year1, year2)
	countMerger.Run()

	replier.Stop()
}
