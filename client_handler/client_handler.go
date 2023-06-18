package main

import (
	"fmt"
	"github.com/google/uuid"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
	"tp1/common/message"
	"tp1/common/middleware"
	"tp1/common/protocol"
)

type ClientHandler struct {
	tripsProducer   *middleware.Producer
	resultsConsumer *middleware.Consumer
	sigtermNotifier chan os.Signal
}

func NewClientHandler(
	tripsProducer *middleware.Producer,
	resultsConsumer *middleware.Consumer,
) *ClientHandler {
	sigtermNotifier := make(chan os.Signal, 1)
	signal.Notify(sigtermNotifier, syscall.SIGTERM)

	return &ClientHandler{
		tripsProducer:   tripsProducer,
		resultsConsumer: resultsConsumer,
		sigtermNotifier: sigtermNotifier,
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
		h.handleConnection(conn)
	}
	h.tripsProducer.Close()
	h.resultsConsumer.Close()
}

func (h *ClientHandler) handleConnection(conn net.Conn) {
	defer conn.Close()
	id := uuid.NewString()
	fmt.Printf("CONNECTION ID = %s\n", id)
	for {
		select {
		case <-h.sigtermNotifier:
			return
		default:
		}
		msg, err := protocol.Recv(conn)
		if err != nil {
			fmt.Printf("[CLIENT %s] Error reading from connection: %v\n", id, err)
			return
		}
		switch msg.Type {
		case protocol.BeginStations:
			h.handleStations(conn, id, msg.Payload)
		case protocol.BeginWeather:
			h.handleWeather(conn, id, msg.Payload)
		case protocol.EndStaticData:
			h.handleEndStaticData(conn)
		case protocol.BeginTrips:
			h.handleTrips(conn, id, msg.Payload)
		case protocol.GetResults:
			h.handleResults(conn)
			return
		}
	}
}

func (h *ClientHandler) handleStations(conn net.Conn, id, city string) {
	protocol.Send(conn, protocol.Message{Type: protocol.Ack, Payload: ""})
	startTime := time.Now()
	h.readBatchesAndSend(conn, id, city, protocol.EndStations, message.StationsBatch, startTime)
	fmt.Printf("[CLIENT %s] Time: %s Finished receiving stations from %s\n", id, time.Since(startTime).String(), city)
}

func (h *ClientHandler) handleWeather(conn net.Conn, id, city string) {
	protocol.Send(conn, protocol.Message{Type: protocol.Ack, Payload: ""})
	startTime := time.Now()
	h.readBatchesAndSend(conn, id, city, protocol.EndWeather, message.WeatherBatch, startTime)
	fmt.Printf("[CLIENT %s] Time: %s Finished receiving weather from %s\n", id, time.Since(startTime).String(), city)
}

func (h *ClientHandler) handleTrips(conn net.Conn, id, city string) {
	protocol.Send(conn, protocol.Message{Type: protocol.Ack, Payload: ""})
	startTime := time.Now()
	h.readBatchesAndSend(conn, id, city, protocol.EndTrips, message.TripsBatch, startTime)
	fmt.Printf("[CLIENT %s] Time: %s Finished receiving trips from %s\n", id, time.Since(startTime).String(), city)
}

func (h *ClientHandler) readBatchesAndSend(conn net.Conn, id string, city string, endMessageType uint8, batchMessageType string, startTime time.Time) {
	batchCounter := 0
	for {
		select {
		case <-h.sigtermNotifier:
			return
		default:
		}
		msg, err := protocol.Recv(conn)
		if err != nil {
			fmt.Printf("[CLIENT %s] Error reading from connection: %v\n", id, err)
			return
		}
		if msg.Type != protocol.Data {
			if msg.Type != endMessageType {
				fmt.Printf("[CLIENT %s] Received invalid message: %v, \n", id, msg.Type)
				return
			}
			protocol.Send(conn, protocol.Message{Type: protocol.Ack, Payload: ""})
			return
		}
		protocol.Send(conn, protocol.Message{Type: protocol.Ack, Payload: ""})
		batchID := strconv.Itoa(batchCounter)
		lines := strings.Split(msg.Payload, ";")
		batchMsg := message.NewBatchMessage(batchMessageType, batchID, city, lines)
		h.tripsProducer.PublishMessage(batchMsg, "")
		if batchCounter%10000 == 0 {
			fmt.Printf("[CLIENT %s] Time: %s Received batch %s\n", id, time.Since(startTime).String(), batchID)
		}
		batchCounter++
	}
}

func (h *ClientHandler) handleEndStaticData(conn net.Conn) {
	stationsEOF := message.NewStationsEOFMessage("1")
	h.tripsProducer.PublishMessage(stationsEOF, "")
	weatherEOF := message.NewWeatherEOFMessage("1")
	h.tripsProducer.PublishMessage(weatherEOF, "")
	protocol.Send(conn, protocol.Message{Type: protocol.Ack, Payload: ""})
}

func (h *ClientHandler) handleResults(conn net.Conn) {
	protocol.Send(conn, protocol.Message{Type: protocol.Ack, Payload: ""})
	tripsEOF := message.NewTripsEOFMessage("1")
	h.tripsProducer.PublishMessage(tripsEOF, "")
	h.resultsConsumer.Consume(func(msg message.Message) {
		if msg.IsEOF() {
			return
		}
		protocol.Send(conn, protocol.NewDataMessage(msg.Batch[0]))
	})

}
