package main

import (
	"fmt"
	"github.com/umahmood/haversine"
	"strconv"
	"strings"
	"time"
	"tp1/common/message"
	"tp1/common/middleware"
)

const (
	startStationNameIndex = iota
	startStationLatitudeIndex
	startStationLongitudeIndex
	endStationNameIndex
	endStationLatitudeIndex
	endStationLongitudeIndex
	yearIndex
)

type DistanceCalculator struct {
	producer  *middleware.Producer
	consumer  *middleware.Consumer
	msgCount  int
	startTime time.Time
}

func NewDistanceCalculator(producer *middleware.Producer, consumer *middleware.Consumer) *DistanceCalculator {
	return &DistanceCalculator{
		producer: producer,
		consumer: consumer,
	}
}

func (c *DistanceCalculator) Run() error {
	c.startTime = time.Now()
	err := c.consumer.Consume(c.processMessage)
	if err != nil {
		return err
	}
	err = c.consumer.Close()
	if err != nil {
		return err
	}
	err = c.producer.Close()
	if err != nil {
		return err
	}
	return nil
}

func (c *DistanceCalculator) processMessage(msg message.Message) error {
	if msg.IsEOF() {
		return c.producer.PublishMessage(msg, "")
	}

	trips := msg.Batch

	tripsWithDistance := c.calculateDistance(trips)

	if len(tripsWithDistance) > 0 {
		tripsWithDistanceBatch := message.NewTripsBatchMessage(msg.ID, msg.ClientID, "", tripsWithDistance)
		err := c.producer.PublishMessage(tripsWithDistanceBatch, "")
		if err != nil {
			return err
		}

		if c.msgCount%20000 == 0 {
			fmt.Printf("[Client %s] Time: %s Received batch %v\n", msg.ClientID, time.Since(c.startTime).String(), msg.ID)
		}
	}

	c.msgCount++
	return nil
}

func (c *DistanceCalculator) calculateDistance(trips []string) []string {
	tripsWithDistance := make([]string, 0, len(trips))
	for _, trip := range trips {
		fields := strings.Split(trip, ",")
		endStationName := fields[endStationNameIndex]

		startStationLatitude, err := strconv.ParseFloat(fields[startStationLatitudeIndex], 64)
		if err != nil {
			fmt.Println(fmt.Errorf("error parsing start station latitude: %w", err))
			continue
		}
		startStationLongitude, err := strconv.ParseFloat(fields[startStationLongitudeIndex], 64)
		if err != nil {
			fmt.Println(fmt.Errorf("error parsing start station longitude: %w", err))
			continue
		}
		endStationLatitude, err := strconv.ParseFloat(fields[endStationLatitudeIndex], 64)
		if err != nil {
			fmt.Println(fmt.Errorf("error parsing end station latitude: %w", err))
			continue
		}
		endStationLongitude, err := strconv.ParseFloat(fields[endStationLongitudeIndex], 64)
		if err != nil {
			fmt.Println(fmt.Errorf("error parsing end station longitude: %w", err))
			continue
		}

		startCoordinates := haversine.Coord{startStationLatitude, startStationLongitude}
		endCoordinates := haversine.Coord{endStationLatitude, endStationLongitude}
		_, distance := haversine.Distance(startCoordinates, endCoordinates)

		tripWithDistance := fmt.Sprintf("%s,%v", endStationName, distance)
		tripsWithDistance = append(tripsWithDistance, tripWithDistance)
	}
	return tripsWithDistance
}
