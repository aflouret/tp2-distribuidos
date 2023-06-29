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

func (f *PrecipitationFilter) Run() error {
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

func (f *PrecipitationFilter) processMessage(msg message.Message) error {
	if msg.IsEOF() {
		return f.producer.PublishMessage(msg, "")
	}

	trips := msg.Batch

	filteredTrips := f.filter(trips)

	if len(filteredTrips) > 0 {
		filteredTripsBatch := message.NewTripsBatchMessage(msg.ID, msg.ClientID, "", filteredTrips)
		err := f.producer.PublishMessage(filteredTripsBatch, "")
		if err != nil {
			return err
		}

		if f.msgCount%20000 == 0 {
			fmt.Printf("[Client %s] Time: %s Received batch %v\n", msg.ClientID, time.Since(f.startTime).String(), msg.ID)
		}
	}

	f.msgCount++
	return nil
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
		if precipitations > 10 {
			startDate := fields[startDateIndex]
			duration := fields[durationIndex]
			filteredTrip := fmt.Sprintf("%s,%s", startDate, duration)
			filteredTrips = append(filteredTrips, filteredTrip)
		}
	}
	return filteredTrips
}
