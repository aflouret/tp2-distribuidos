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
		log.Panic(err)
	}
	producer, err := middleware.NewProducer("producer")
	if err != nil {
		log.Panic(err)
	}

	return &ConnectionHandler{
		id:              id,
		producer:        producer,
		resultsConsumer: resultsConsumer,
		conn:            conn,
		sigtermNotifier: sigtermNotifier,
	}
}

func (h *ConnectionHandler) Close() error {
	clientEOF := message.NewClientEOFMessage(h.id)
	err := h.producer.PublishMessage(clientEOF, "")
	if err != nil {
		return err
	}
	err = h.producer.Close()
	if err != nil {
		return err
	}
	err = h.resultsConsumer.Close()
	if err != nil {
		return err
	}
	return h.conn.Close()
}

func (h *ConnectionHandler) Run() error {
	for {
		select {
		case <-h.sigtermNotifier:
			return nil
		default:
		}
		msg, err := protocol.Recv(h.conn)
		if err != nil {
			fmt.Printf("[CLIENT %s] Error reading from connection: %v\n", h.id, err)
			return err
		}
		switch msg.Type {
		case protocol.BeginStations:
			err = h.handleStations(msg.Payload)
		case protocol.BeginWeather:
			err = h.handleWeather(msg.Payload)
		case protocol.EndStaticData:
			err = h.handleEndStaticData()
		case protocol.BeginTrips:
			err = h.handleTrips(msg.Payload)
		case protocol.GetResults:
			return h.handleResults()
		}
		if err != nil {
			return err
		}
	}
}

func (h *ConnectionHandler) handleStations(city string) error {
	err := protocol.Send(h.conn, protocol.Message{Type: protocol.Ack, Payload: ""})
	if err != nil {
		return err
	}
	startTime := time.Now()
	err = h.readBatchesAndSend(city, protocol.EndStations, message.StationsBatch, startTime)
	if err != nil {
		return err
	}
	fmt.Printf("[CLIENT %s] Time: %s Finished receiving stations from %s\n", h.id, time.Since(startTime).String(), city)
	return nil
}

func (h *ConnectionHandler) handleWeather(city string) error {
	err := protocol.Send(h.conn, protocol.Message{Type: protocol.Ack, Payload: ""})
	if err != nil {
		return err
	}
	startTime := time.Now()
	err = h.readBatchesAndSend(city, protocol.EndWeather, message.WeatherBatch, startTime)
	if err != nil {
		return err
	}
	fmt.Printf("[CLIENT %s] Time: %s Finished receiving weather from %s\n", h.id, time.Since(startTime).String(), city)
	return nil
}

func (h *ConnectionHandler) handleTrips(city string) error {
	err := protocol.Send(h.conn, protocol.Message{Type: protocol.Ack, Payload: ""})
	if err != nil {
		return err
	}
	startTime := time.Now()
	err = h.readBatchesAndSend(city, protocol.EndTrips, message.TripsBatch, startTime)
	if err != nil {
		return err
	}
	fmt.Printf("[CLIENT %s] Time: %s Finished receiving trips from %s\n", h.id, time.Since(startTime).String(), city)
	return nil
}

func (h *ConnectionHandler) readBatchesAndSend(city string, endMessageType uint8, batchMessageType string, startTime time.Time) error {
	messageCounter := 1
	currentBatch := make([]string, 0, 2000)
	for {
		select {
		case <-h.sigtermNotifier:
			return nil
		default:
		}

		msg, err := protocol.Recv(h.conn)
		if err != nil {
			fmt.Printf("[CLIENT %s] Error reading from connection: %v\n", h.id, err)
			return err
		}
		if msg.Type != protocol.Data {
			if msg.Type != endMessageType {
				fmt.Printf("[CLIENT %s] Received invalid message: %v, \n", h.id, msg.Type)
				return err
			}

			if len(currentBatch) > 0 {
				batchID := strconv.Itoa(h.batchCounter)
				batchMsg := message.NewBatchMessage(batchMessageType, batchID, h.id, city, currentBatch)
				err = h.producer.PublishMessage(batchMsg, "")
				if err != nil {
					return err
				}
				h.batchCounter++
			}

			return protocol.Send(h.conn, protocol.Message{Type: protocol.Ack, Payload: ""})

		}
		err = protocol.Send(h.conn, protocol.Message{Type: protocol.Ack, Payload: ""})
		if err != nil {
			return err
		}

		lines := strings.Split(msg.Payload, ";")
		currentBatch = append(currentBatch, lines...)
		if messageCounter%4 == 0 {
			batchID := strconv.Itoa(h.batchCounter)
			batchMsg := message.NewBatchMessage(batchMessageType, batchID, h.id, city, currentBatch)
			err = h.producer.PublishMessage(batchMsg, "")
			if err != nil {
				return err
			}

			h.batchCounter++
			currentBatch = make([]string, 0, 2000)
		}
		if messageCounter%10000 == 0 {
			fmt.Printf("[CLIENT %s] Time: %s Received batch %v\n", h.id, time.Since(startTime).String(), messageCounter)
		}
		messageCounter++
	}
}

func (h *ConnectionHandler) handleEndStaticData() error {
	stationsEOF := message.NewStationsEOFMessage(h.id)
	err := h.producer.PublishMessage(stationsEOF, "")
	if err != nil {
		return err
	}
	weatherEOF := message.NewWeatherEOFMessage(h.id)
	err = h.producer.PublishMessage(weatherEOF, "")
	if err != nil {
		return err
	}
	return protocol.Send(h.conn, protocol.Message{Type: protocol.Ack, Payload: ""})
}

func (h *ConnectionHandler) handleResults() error {
	err := protocol.Send(h.conn, protocol.Message{Type: protocol.Ack, Payload: ""})
	if err != nil {
		return err
	}
	tripsEOF := message.NewTripsEOFMessage(h.id)
	err = h.producer.PublishMessage(tripsEOF, "")
	if err != nil {
		return err
	}
	return h.resultsConsumer.Consume(func(msg message.Message) error {
		if msg.IsEOF() {
			return nil
		}
		return protocol.Send(h.conn, protocol.NewDataMessage(msg.Batch[0]))
	})
}
