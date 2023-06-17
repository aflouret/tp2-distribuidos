package main

import (
	"log"
	"tp1/common/middleware"
)

func main() {
	resultsConsumer, err := middleware.NewConsumer("consumer")
	if err != nil {
		log.Fatal(err)
	}
	producer, err := middleware.NewProducer("producer")
	if err != nil {
		log.Fatal(err)
	}

	clientHandler := NewClientHandler(producer, resultsConsumer)
	clientHandler.Run()
}
