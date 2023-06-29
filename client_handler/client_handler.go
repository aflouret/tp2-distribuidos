package main

import (
	"context"
	"fmt"
	"golang.org/x/sync/semaphore"
	"net"
	"tp1/common/message"
	"tp1/common/middleware"
)

const MAXCLIENTS = 4

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
	defer func() {
		err := listener.Close()
		if err != nil {
			fmt.Println("Error closing listener", err)
		}
	}()

	var sem = semaphore.NewWeighted(MAXCLIENTS)
	var ctx = context.TODO()

	fmt.Println("ClientHandler listening on port 12380")
	for {

		err := sem.Acquire(ctx, 1)
		if err != nil {
			return err
		}

		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}

		go func() {
			handleConnection(conn)
			sem.Release(1)
		}()
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
	err = producer.PublishMessage(message.NewClientEOFMessage(message.AllClients), "")
	if err != nil {
		return err
	}
	// Await response from last stage to confirm it has been received by all nodes
	err = consumer.Consume(func(msg message.Message) error { return nil })
	if err != nil {
		return err
	}
	err = producer.Close()
	if err != nil {
		return err
	}
	err = consumer.Close()
	if err != nil {
		return err
	}

	return nil
}

func handleConnection(conn net.Conn) {
	h := NewConnectionHandler(conn)

	err := h.Run()
	if err != nil {
		fmt.Printf("Connection %s ended with error: %s\n", h.id, err)
		return
	}
	err = h.Close()
	if err != nil {
		fmt.Printf("Connection %s closed with error: %s\n", h.id, err)
		return
	}
	fmt.Printf("Connection %s closed successfully\n", h.id)
}
