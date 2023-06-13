package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"tp1/common/middleware"
	"tp1/common/protocol"
)

type ClientHandler struct {
	tripsProducer    *middleware.Producer
	stationsProducer *middleware.Producer
	weatherProducer  *middleware.Producer
	resultsConsumer  *middleware.Consumer
	sigtermNotifier  chan os.Signal
}

func NewClientHandler(
	tripsProducer *middleware.Producer,
	stationsProducer *middleware.Producer,
	weatherProducer *middleware.Producer,
	resultsConsumer *middleware.Consumer,
) *ClientHandler {
	sigtermNotifier := make(chan os.Signal, 1)
	signal.Notify(sigtermNotifier, syscall.SIGTERM)

	return &ClientHandler{
		tripsProducer:    tripsProducer,
		stationsProducer: stationsProducer,
		weatherProducer:  weatherProducer,
		resultsConsumer:  resultsConsumer,
		sigtermNotifier:  sigtermNotifier,
	}
}

func (h *ClientHandler) Run() {
	listener, err := net.Listen("tcp", ":12345")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	fmt.Println("ClientHandler listening on port 12345")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}
		fmt.Printf("New connection from: %v\n", conn.RemoteAddr())
		if shouldExit := h.handleConnection(conn); shouldExit {
			break
		}
	}
	h.tripsProducer.Close()
	h.resultsConsumer.Close()
}

func (h *ClientHandler) handleConnection(conn net.Conn) (shouldExit bool) {
	defer conn.Close()

	msg, err := protocol.Recv(conn)
	if err != nil {
		fmt.Printf("Error reading from connection: %v\n", err)
		return
	}
	switch msg.Type {
	case protocol.BeginStations:
		shouldExit = h.handleStations(conn, msg.Payload)
	case protocol.BeginWeather:
		shouldExit = h.handleWeather(conn, msg.Payload)
	case protocol.EndStaticData:
		h.handleEndStaticData(conn)
	case protocol.BeginTrips:
		shouldExit = h.handleTrips(conn, msg.Payload)
	case protocol.GetResults:
		h.handleResults(conn)
		shouldExit = true
	}
	return
}

func (h *ClientHandler) handleStations(conn net.Conn, city string) (shouldExit bool) {
	protocol.Send(conn, protocol.Message{Type: protocol.Ack, Payload: ""})
	startTime := time.Now()
	shouldExit = h.readBatchesAndSend(conn, city, h.stationsProducer, protocol.EndStations, startTime)
	fmt.Printf("Time: %s Finished receiving stations from %s\n", time.Since(startTime).String(), city)
	return
}

func (h *ClientHandler) handleWeather(conn net.Conn, city string) (shouldExit bool) {
	protocol.Send(conn, protocol.Message{Type: protocol.Ack, Payload: ""})
	startTime := time.Now()
	shouldExit = h.readBatchesAndSend(conn, city, h.weatherProducer, protocol.EndWeather, startTime)
	fmt.Printf("Time: %s Finished receiving weather from %s\n", time.Since(startTime).String(), city)
	return
}

func (h *ClientHandler) handleTrips(conn net.Conn, city string) (shouldExit bool) {
	protocol.Send(conn, protocol.Message{Type: protocol.Ack, Payload: ""})
	startTime := time.Now()
	shouldExit = h.readBatchesAndSend(conn, city, h.tripsProducer, protocol.EndTrips, startTime)
	fmt.Printf("Time: %s Finished receiving trips from %s\n", time.Since(startTime).String(), city)
	return
}

func (h *ClientHandler) readBatchesAndSend(conn net.Conn, city string, producer *middleware.Producer, endMessageType uint8, startTime time.Time) (shouldExit bool) {
	batchCounter := 0
	for {
		select {
		case <-h.sigtermNotifier:
			shouldExit = true
			return
		default:
		}
		msg, err := protocol.Recv(conn)
		if err != nil {
			fmt.Printf("Error reading from connection: %v\n", err)
			return
		}
		if msg.Type != protocol.Data {
			if msg.Type != endMessageType {
				fmt.Printf("Received invalid message: %v, \n", msg.Type)
				return
			}
			protocol.Send(conn, protocol.Message{Type: protocol.Ack, Payload: ""})
			return
		}
		protocol.Send(conn, protocol.Message{Type: protocol.Ack, Payload: ""})
		id := strconv.Itoa(batchCounter)
		batch := id + "," + city + "\n" + msg.Payload
		producer.PublishMessage(batch, "")
		if batchCounter%10000 == 0 {
			fmt.Printf("Time: %s Received batch %s\n", time.Since(startTime).String(), id)
		}
		batchCounter++
	}
}

func (h *ClientHandler) handleEndStaticData(conn net.Conn) {
	h.stationsProducer.PublishMessage("eof", "")
	h.weatherProducer.PublishMessage("eof", "")
	protocol.Send(conn, protocol.Message{Type: protocol.Ack, Payload: ""})
}

func (h *ClientHandler) handleResults(conn net.Conn) {
	protocol.Send(conn, protocol.Message{Type: protocol.Ack, Payload: ""})
	h.tripsProducer.PublishMessage("eof", "")
	h.resultsConsumer.Consume(func(msg string) {
		protocol.Send(conn, protocol.NewDataMessage(msg))
	})

}
