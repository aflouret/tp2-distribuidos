package main

import (
	"fmt"
	"strings"
	"time"
	"tp1/common/message"
	"tp1/common/middleware"
)

const (
	startStationNameIndex = iota
	yearIndex
)

type YearFilter struct {
	producer  *middleware.Producer
	consumer  *middleware.Consumer
	year1     string
	year2     string
	msgCount  int
	startTime time.Time
}

func NewYearFilter(producer *middleware.Producer, consumer *middleware.Consumer, year1 string, year2 string) *YearFilter {
	return &YearFilter{
		producer: producer,
		consumer: consumer,
		year1:    year1,
		year2:    year2,
	}
}

func (f *YearFilter) Run() {
	defer f.consumer.Close()
	defer f.producer.Close()
	f.startTime = time.Now()

	f.consumer.Consume(f.processMessage)
}

func (f *YearFilter) processMessage(msg message.Message) {
	if msg.IsEOF() {
		f.producer.PublishMessage(msg, "")
		return
	}
	trips := msg.Batch

	if f.msgCount%20000 == 0 {
		fmt.Printf("Time: %s Received batch %v\n", time.Since(f.startTime).String(), msg.ID)
	}

	filteredTrips := f.filter(trips)

	if len(filteredTrips) > 0 {
		filteredTripsBatch := message.NewTripsBatchMessage(msg.ID, "", filteredTrips)

		f.producer.PublishMessage(filteredTripsBatch, "")
	}

	f.msgCount++
}

func (f *YearFilter) filter(trips []string) []string {
	filteredTrips := make([]string, 0, len(trips))

	for _, trip := range trips {
		fields := strings.Split(trip, ",")
		year := fields[yearIndex]
		if year == f.year1 || year == f.year2 {
			startStationName := fields[startStationNameIndex]
			filteredTrip := year + "," + startStationName
			filteredTrips = append(filteredTrips, filteredTrip)
		}
	}
	return filteredTrips
}
