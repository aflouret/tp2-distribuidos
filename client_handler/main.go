package main

import (
	log "github.com/sirupsen/logrus"
	"tp1/common/checkreplier"
)

func main() {

	replier := checkreplier.NewReplier()
	err := replier.Run()
	if err != nil {
		log.Fatal(err)
	}

	clientHandler := NewClientHandler()
	clientHandler.Run()

	replier.Stop()
}
