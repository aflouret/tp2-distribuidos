package main

import "os"

func main() {
	address := os.Getenv("ADDRESS")

	clientHandler := NewClientHandler(address)
	if err := clientHandler.Run(); err != nil {
		panic(err)
	}
}
