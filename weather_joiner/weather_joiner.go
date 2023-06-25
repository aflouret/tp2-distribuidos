package main

import (
	"fmt"
	"strings"
	"time"
	"tp1/common/message"
	"tp1/common/middleware"
)

const (
	tripStartDateIndex = iota
	tripDurationIndex
)

type WeatherJoiner struct {
	producer                   *middleware.Producer
	consumer                   *middleware.Consumer
	precipitationsByDateByCity map[string]map[string]map[string]string
	msgCount                   int
	startTime                  time.Time
}

func NewWeatherJoiner(
	producer *middleware.Producer,
	consumer *middleware.Consumer,
) *WeatherJoiner {
	precipitationsByDateByCity := make(map[string]map[string]map[string]string)
	return &WeatherJoiner{
		producer:                   producer,
		consumer:                   consumer,
		precipitationsByDateByCity: precipitationsByDateByCity,
		startTime:                  time.Now(),
	}
}

func (j *WeatherJoiner) Run() {
	defer j.producer.Close()

	j.consumer.Consume(j.processMessage)
	j.consumer.Close()
}

func (j *WeatherJoiner) processMessage(msg message.Message) {

	switch msg.MsgType {
	case message.WeatherBatch, message.WeatherEOF:
		j.processWeatherMessage(msg)
	case message.TripsBatch, message.TripsEOF:
		j.processTripsMessage(msg)
	}
}

func (j *WeatherJoiner) processWeatherMessage(msg message.Message) {
	if msg.IsEOF() {
		return
	}

	if _, ok := j.precipitationsByDateByCity[msg.ClientID]; !ok {
		j.precipitationsByDateByCity[msg.ClientID] = make(map[string]map[string]string)
	}
	if _, ok := j.precipitationsByDateByCity[msg.ClientID][msg.City]; !ok {
		j.precipitationsByDateByCity[msg.ClientID][msg.City] = make(map[string]string)
	}

	weather := msg.Batch
	for _, w := range weather {
		fields := strings.Split(w, ",")
		date := fields[0]
		precipitations := fields[1]

		previousDate, err := getPreviousDate(date)
		if err != nil {
			fmt.Printf("error parsing date: %s", err)
			return
		}
		j.precipitationsByDateByCity[msg.ClientID][msg.City][previousDate] = precipitations
	}
}

func (j *WeatherJoiner) processTripsMessage(msg message.Message) {
	if msg.IsEOF() {
		j.producer.PublishMessage(msg, "")
		delete(j.precipitationsByDateByCity, msg.ClientID)
		return
	}
	trips := msg.Batch
	joinedTrips := j.joinWeather(msg.City, trips, msg.ClientID)
	if len(joinedTrips) > 0 {
		joinedTripsBatch := message.NewTripsBatchMessage(msg.ID, msg.ClientID, "", joinedTrips)
		j.producer.PublishMessage(joinedTripsBatch, "")
		if j.msgCount%20000 == 0 {
			fmt.Printf("[Client %s] Time: %s Received batch %v\n", msg.ClientID, time.Since(j.startTime).String(), msg.ID)
		}
	}
	j.msgCount++
}

func (j *WeatherJoiner) joinWeather(city string, trips []string, clientID string) []string {
	joinedTrips := make([]string, 0, len(trips))
	precipitationsByDateByCity := j.precipitationsByDateByCity[clientID]
	for _, trip := range trips {
		tripFields := strings.Split(trip, ",")
		startDate := tripFields[tripStartDateIndex]
		duration := tripFields[tripDurationIndex]

		precipitations, ok := precipitationsByDateByCity[city][startDate]
		if !ok {
			continue
		}

		joinedTrip := fmt.Sprintf("%s,%s,%s",
			startDate,
			duration,
			precipitations)
		joinedTrips = append(joinedTrips, joinedTrip)
	}
	return joinedTrips
}
