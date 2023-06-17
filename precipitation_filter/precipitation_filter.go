package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"tp1/common/message"
	"tp1/common/middleware"
)

const (
	startDateIndex = iota
	durationIndex
	precipitationsIndex
)

type PrecipitationFilter struct {
	producer              *middleware.Producer
	consumer              *middleware.Consumer
	minimumPrecipitations float64
	msgCount              int
	startTime             time.Time
}

func NewPrecipitationFilter(consumer *middleware.Consumer, producer *middleware.Producer, minimumPrecipitations float64) *PrecipitationFilter {
	return &PrecipitationFilter{
		producer:              producer,
		consumer:              consumer,
		minimumPrecipitations: minimumPrecipitations,
	}
}

func (f *PrecipitationFilter) Run() {
	defer f.consumer.Close()
	defer f.producer.Close()
	f.startTime = time.Now()

	f.consumer.Consume(f.processMessage)
}

func (f *PrecipitationFilter) processMessage(msg message.Message) {
	if msg.IsEOF() {
		f.producer.PublishMessage(msg, "")
		return
	}

	trips := msg.Batch

	filteredTrips := f.filter(trips)

	if len(filteredTrips) > 0 {
		filteredTripsBatch := message.NewTripsBatchMessage(msg.ID, "", filteredTrips)
		f.producer.PublishMessage(filteredTripsBatch, "")

		if f.msgCount%20000 == 0 {
			fmt.Printf("Time: %s Received batch %v\n", time.Since(f.startTime).String(), msg.ID)
		}
	}

	f.msgCount++

}

func (f *PrecipitationFilter) filter(trips []string) []string {
	filteredTrips := make([]string, 0, len(trips))
	for _, trip := range trips {
		fields := strings.Split(trip, ",")
		precipitationsString := fields[precipitationsIndex]
		precipitations, err := strconv.ParseFloat(precipitationsString, 64)
		if err != nil {
			fmt.Println(fmt.Errorf("error parsing precipitations: %w", err))
			continue
		}
		if precipitations > f.minimumPrecipitations {
			startDate := fields[startDateIndex]
			duration := fields[durationIndex]
			filteredTrip := fmt.Sprintf("%s,%s", startDate, duration)
			filteredTrips = append(filteredTrips, filteredTrip)
		}
	}
	return filteredTrips
}
