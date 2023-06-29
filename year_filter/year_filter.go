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

func (f *YearFilter) Run() error {
	f.startTime = time.Now()
	err := f.consumer.Consume(f.processMessage)
	if err != nil {
		return err
	}
	err = f.consumer.Close()
	if err != nil {
		return err
	}
	err = f.producer.Close()
	if err != nil {
		return err
	}
	return nil
}

func (f *YearFilter) processMessage(msg message.Message) error {
	if msg.IsEOF() {
		return f.producer.PublishMessage(msg, "")
	}
	trips := msg.Batch

	if f.msgCount%20000 == 0 {
		fmt.Printf("[Client %s] Time: %s Received batch %v\n", msg.ClientID, time.Since(f.startTime).String(), msg.ID)
	}

	filteredTrips := f.filter(trips)

	if len(filteredTrips) > 0 {
		filteredTripsBatch := message.NewTripsBatchMessage(msg.ID, msg.ClientID, "", filteredTrips)

		err := f.producer.PublishMessage(filteredTripsBatch, "")
		if err != nil {
			return err
		}
	}

	f.msgCount++
	return nil
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
