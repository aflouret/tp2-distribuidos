package main

import (
	"fmt"
	"net"
	"tp1/common/message"
	"tp1/common/middleware"
)

type ClientHandler struct {
	address string
}

func NewClientHandler(address string) *ClientHandler {
	return &ClientHandler{
		address: address,
	}
}

func (h *ClientHandler) Run() error {
	err := resetState()
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", h.address)
	if err != nil {
		return err
	}
	defer listener.Close()

	fmt.Println("ClientHandler listening on port 12345")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}
		go handleConnection(conn)
	}

}

func resetState() error {
	producer, err := middleware.NewProducer("producer")
	if err != nil {
		return err
	}
	consumer, err := middleware.NewConsumer("consumer", message.AllClients)
	if err != nil {
		return err
	}
	// Send EOF message to clear resources from all nodes
	producer.PublishMessage(message.NewClientEOFMessage(message.AllClients), "")
	// Await response from last stage to confirm it has been received by all nodes
	consumer.Consume(func(msg message.Message) {})
	producer.Close()
	consumer.Close()

	return nil
}

func handleConnection(conn net.Conn) {
	h := NewConnectionHandler(conn)
	defer h.Close()
	err := h.Run()
	if err != nil {
		fmt.Printf("Connection %s closed with error: %s\n", h.id, err)
		return
	}
	fmt.Printf("Connection %s closed successfully\n", h.id)
}
