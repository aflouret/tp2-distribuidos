package main

import (
	"fmt"
	"strings"
	"time"
	"tp1/common/message"
	"tp1/common/middleware"
)

const (
	yearIndex = iota
	startStationNameIndex
)

type TripCounter struct {
	producer            *middleware.Producer
	consumer            *middleware.Consumer
	year1               string
	year2               string
	countByStationYear1 map[string]map[string]int
	countByStationYear2 map[string]map[string]int
	msgCount            int
	startTime           time.Time
}

func NewTripCounter(year1 string, year2 string, consumer *middleware.Consumer, producer *middleware.Producer) *TripCounter {
	countByStationYear1 := make(map[string]map[string]int)
	countByStationYear2 := make(map[string]map[string]int)
	return &TripCounter{
		producer:            producer,
		consumer:            consumer,
		countByStationYear1: countByStationYear1,
		countByStationYear2: countByStationYear2,
		year1:               year1,
		year2:               year2,
	}
}

func (a *TripCounter) Run() {
	defer a.consumer.Close()
	defer a.producer.Close()
	a.startTime = time.Now()

	a.consumer.Consume(a.processMessage)
}

func (a *TripCounter) processMessage(msg message.Message) {
	if msg.IsEOF() {
		a.sendResults(msg.ClientID)
		delete(a.countByStationYear1, msg.ClientID)
		delete(a.countByStationYear2, msg.ClientID)
		return
	}

	a.updateCount(msg)

	if a.msgCount%20000 == 0 {
		fmt.Printf("[Client %s] Time: %s Received batch %v\n", msg.ClientID, time.Since(a.startTime).String(), msg.ID)
	}
	a.msgCount++
}

func (a *TripCounter) updateCount(msg message.Message) {
	trips := msg.Batch
	countByStationYear1, ok := a.countByStationYear1[msg.ClientID]
	if !ok {
		countByStationYear1 = make(map[string]int)
	}
	countByStationYear2, ok := a.countByStationYear2[msg.ClientID]
	if !ok {
		countByStationYear2 = make(map[string]int)
	}

	for _, trip := range trips {
		fields := strings.Split(trip, ",")
		year := fields[yearIndex]
		startStationName := fields[startStationNameIndex]
		if year == a.year1 {
			if c, ok := countByStationYear1[startStationName]; ok {
				countByStationYear1[startStationName] = c + 1
			} else {
				countByStationYear1[startStationName] = 1
			}
		} else if year == a.year2 {
			if c, ok := countByStationYear2[startStationName]; ok {
				countByStationYear2[startStationName] = c + 1
			} else {
				countByStationYear2[startStationName] = 1
			}
		}
	}
	a.countByStationYear1[msg.ClientID] = countByStationYear1
	a.countByStationYear2[msg.ClientID] = countByStationYear2
}

func (a *TripCounter) sendResults(clientID string) {
	for k, v := range a.countByStationYear1[clientID] {
		result := fmt.Sprintf("%s,%s,%v", a.year1, k, v)
		msg := message.NewTripsBatchMessage("", clientID, "", []string{result})
		a.producer.PublishMessage(msg, "")
	}
	for k, v := range a.countByStationYear2[clientID] {
		result := fmt.Sprintf("%s,%s,%v", a.year2, k, v)
		msg := message.NewTripsBatchMessage("", clientID, "", []string{result})
		a.producer.PublishMessage(msg, "")
	}
	eof := message.NewTripsEOFMessage("1", clientID)
	a.producer.PublishMessage(eof, "")
}
