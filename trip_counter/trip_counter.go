package main

import (
	"fmt"
	"strings"
	"time"
	"tp1/common/middleware"
	"tp1/common/utils"
)

const (
	startStationNameIndex = iota
)

type TripCounter struct {
	producer       *middleware.Producer
	consumer       *middleware.Consumer
	countByStation map[string]int
	year           string
	msgCount       int
	startTime      time.Time
}

func NewTripCounter(year string, consumer *middleware.Consumer, producer *middleware.Producer) *TripCounter {
	countByStation := make(map[string]int)

	return &TripCounter{
		producer:       producer,
		consumer:       consumer,
		countByStation: countByStation,
		year:           year,
	}
}

func (a *TripCounter) Run() {
	defer a.consumer.Close()
	defer a.producer.Close()
	a.startTime = time.Now()

	a.consumer.Consume(a.processMessage)
	a.sendResults()
}

func (a *TripCounter) processMessage(msg string) {
	if msg == "eof" {
		return
	}

	id, _, trips := utils.ParseBatch(msg)

	a.updateCount(trips)

	if a.msgCount%20000 == 0 {
		fmt.Printf("Time: %s Received batch %v\n", time.Since(a.startTime).String(), id)
	}
	a.msgCount++
}

func (a *TripCounter) updateCount(trips []string) {
	for _, trip := range trips {
		fields := strings.Split(trip, ",")
		startStationName := fields[startStationNameIndex]
		if c, ok := a.countByStation[startStationName]; ok {
			a.countByStation[startStationName] = c + 1
		} else {
			a.countByStation[startStationName] = 1
		}
	}
}

func (a *TripCounter) sendResults() {
	for k, v := range a.countByStation {
		result := fmt.Sprintf("%s,%s,%v", a.year, k, v)
		a.producer.PublishMessage(result, "")
	}
	a.producer.PublishMessage("eof", "")
}
