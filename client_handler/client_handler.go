package main

import (
	"fmt"
	"net"
	"tp1/common/message"
	"tp1/common/middleware"
	"tp1/common/protocol"
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

	for {
		select {
		case <-h.sigtermNotifier:
			return
		default:
		}
		msg, err := protocol.Recv(conn)
		if err != nil {
			fmt.Printf("[CLIENT %s] Error reading from connection: %v\n", h.id, err)
			return
		}
		switch msg.Type {
		case protocol.BeginStations:
			h.handleStations(msg.Payload)
		case protocol.BeginWeather:
			h.handleWeather(msg.Payload)
		case protocol.EndStaticData:
			h.handleEndStaticData()
		case protocol.BeginTrips:
			h.handleTrips(msg.Payload)
		case protocol.GetResults:
			h.handleResults()
			return
		}
	}
}
