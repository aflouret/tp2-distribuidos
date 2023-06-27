package main

import (
	"fmt"
	"github.com/google/uuid"
	"log"
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

type ConnectionHandler struct {
	id              string
	sigtermNotifier chan os.Signal
	producer        *middleware.Producer
	resultsConsumer *middleware.Consumer
	conn            net.Conn
	batchCounter    int
}

func NewConnectionHandler(conn net.Conn) *ConnectionHandler {
	id := uuid.NewString()
	fmt.Printf("New connection from: %v - ClientID: %s\n", conn.RemoteAddr(), id)

	sigtermNotifier := make(chan os.Signal, 1)
	signal.Notify(sigtermNotifier, syscall.SIGTERM)

	resultsConsumer, err := middleware.NewConsumer("consumer", id)
	if err != nil {
		log.Fatal(err)
	}
	producer, err := middleware.NewProducer("producer")
	if err != nil {
		log.Fatal(err)
	}

	return &ConnectionHandler{
		id:              id,
		producer:        producer,
		resultsConsumer: resultsConsumer,
		conn:            conn,
		sigtermNotifier: sigtermNotifier,
	}
}

func (h *ConnectionHandler) Close() {
	h.producer.Close()
	h.resultsConsumer.Close()
	h.conn.Close()
}

func (h *ConnectionHandler) handleStations(city string) {
	protocol.Send(h.conn, protocol.Message{Type: protocol.Ack, Payload: ""})
	startTime := time.Now()
	h.readBatchesAndSend(city, protocol.EndStations, message.StationsBatch, startTime)
	fmt.Printf("[CLIENT %s] Time: %s Finished receiving stations from %s\n", h.id, time.Since(startTime).String(), city)
}

func (h *ConnectionHandler) handleWeather(city string) {
	protocol.Send(h.conn, protocol.Message{Type: protocol.Ack, Payload: ""})
	startTime := time.Now()
	h.readBatchesAndSend(city, protocol.EndWeather, message.WeatherBatch, startTime)
	fmt.Printf("[CLIENT %s] Time: %s Finished receiving weather from %s\n", h.id, time.Since(startTime).String(), city)
}

func (h *ConnectionHandler) handleTrips(city string) {
	protocol.Send(h.conn, protocol.Message{Type: protocol.Ack, Payload: ""})
	startTime := time.Now()
	h.readBatchesAndSend(city, protocol.EndTrips, message.TripsBatch, startTime)
	fmt.Printf("[CLIENT %s] Time: %s Finished receiving trips from %s\n", h.id, time.Since(startTime).String(), city)
}

func (h *ConnectionHandler) readBatchesAndSend(city string, endMessageType uint8, batchMessageType string, startTime time.Time) {
	for {
		select {
		case <-h.sigtermNotifier:
			return
		default:
		}
		msg, err := protocol.Recv(h.conn)
		if err != nil {
			fmt.Printf("[CLIENT %s] Error reading from connection: %v\n", h.id, err)
			return
		}
		if msg.Type != protocol.Data {
			if msg.Type != endMessageType {
				fmt.Printf("[CLIENT %s] Received invalid message: %v, \n", h.id, msg.Type)
				return
			}
			protocol.Send(h.conn, protocol.Message{Type: protocol.Ack, Payload: ""})
			return
		}
		protocol.Send(h.conn, protocol.Message{Type: protocol.Ack, Payload: ""})
		batchID := strconv.Itoa(h.batchCounter)
		lines := strings.Split(msg.Payload, ";")
		batchMsg := message.NewBatchMessage(batchMessageType, batchID, h.id, city, lines)
		h.producer.PublishMessage(batchMsg, "")
		if h.batchCounter%10000 == 0 {
			fmt.Printf("[CLIENT %s] Time: %s Received batch %s\n", h.id, time.Since(startTime).String(), batchID)
		}
		h.batchCounter++
	}
}

func (h *ConnectionHandler) handleEndStaticData() {
	stationsEOF := message.NewStationsEOFMessage(h.id)
	h.producer.PublishMessage(stationsEOF, "")
	weatherEOF := message.NewWeatherEOFMessage(h.id)
	h.producer.PublishMessage(weatherEOF, "")
	protocol.Send(h.conn, protocol.Message{Type: protocol.Ack, Payload: ""})
}

func (h *ConnectionHandler) handleResults() {
	protocol.Send(h.conn, protocol.Message{Type: protocol.Ack, Payload: ""})
	tripsEOF := message.NewTripsEOFMessage(h.id)
	h.producer.PublishMessage(tripsEOF, "")
	h.resultsConsumer.Consume(func(msg message.Message) {
		if msg.IsEOF() {
			return
		}
		protocol.Send(h.conn, protocol.NewDataMessage(msg.Batch[0]))
	})
	clientEOF := message.NewClientEOFMessage(h.id)
	h.producer.PublishMessage(clientEOF, "")
}
