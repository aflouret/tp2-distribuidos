package main

import (
	log "github.com/sirupsen/logrus"
	"os"
	"tp1/common/checkreplier"
)

func main() {
	address := os.Getenv("ADDRESS")

	replier := checkreplier.NewReplier()
	err := replier.Run()
	if err != nil {
		log.Fatal(err)
	}

	clientHandler := NewClientHandler(address)
	if err := clientHandler.Run(); err != nil {
		panic(err)
	}
}
