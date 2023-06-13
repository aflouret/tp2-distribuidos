package main

import (
	"fmt"
	"strings"
	"time"
	"tp1/common/middleware"
	"tp1/common/utils"
)

const (
	tripStartDateIndex = iota
	tripDurationIndex
)

type WeatherJoiner struct {
	producer                   *middleware.Producer
	tripsConsumer              *middleware.Consumer
	weatherConsumer            *middleware.Consumer
	precipitationsByDateByCity map[string]map[string]string
	msgCount                   int
	startTime                  time.Time
}

func NewWeatherJoiner(
	producer *middleware.Producer,
	weatherConsumer *middleware.Consumer,
	tripsConsumer *middleware.Consumer,
) *WeatherJoiner {
	precipitationsByDateByCity := make(map[string]map[string]string)
	return &WeatherJoiner{
		producer:                   producer,
		tripsConsumer:              tripsConsumer,
		weatherConsumer:            weatherConsumer,
		precipitationsByDateByCity: precipitationsByDateByCity,
		startTime:                  time.Now(),
	}
}

func (j *WeatherJoiner) Run() {
	defer j.producer.Close()

	j.weatherConsumer.Consume(j.processWeatherMessage)
	j.weatherConsumer.Close()
	j.tripsConsumer.Consume(j.processTripMessage)
	j.tripsConsumer.Close()
}

func (j *WeatherJoiner) processWeatherMessage(msg string) {
	if msg == "eof" {
		return
	}

	_, city, weather := utils.ParseBatch(msg)

	for _, w := range weather {
		fields := strings.Split(w, ",")
		date := fields[0]
		precipitations := fields[1]

		previousDate, err := getPreviousDate(date)
		if err != nil {
			fmt.Printf("error parsing date: %s", err)
			return
		}

		if _, ok := j.precipitationsByDateByCity[city]; !ok {
			j.precipitationsByDateByCity[city] = make(map[string]string)
		}
		j.precipitationsByDateByCity[city][previousDate] = precipitations
	}
}

func (j *WeatherJoiner) processTripMessage(msg string) {
	if msg == "eof" {
		j.producer.PublishMessage(msg, "")
		return
	}
	id, city, trips := utils.ParseBatch(msg)
	joinedTrips := j.joinWeather(city, trips)
	if len(joinedTrips) > 0 {
		joinedTripsBatch := utils.CreateBatch(id, "", joinedTrips)
		j.producer.PublishMessage(joinedTripsBatch, "")
		if j.msgCount%20000 == 0 {
			fmt.Printf("Time: %s Received batch %v\n", time.Since(j.startTime).String(), id)
		}
	}

	j.msgCount++
}

func (j *WeatherJoiner) joinWeather(city string, trips []string) []string {
	joinedTrips := make([]string, 0, len(trips))
	for _, trip := range trips {
		tripFields := strings.Split(trip, ",")
		startDate := tripFields[tripStartDateIndex]
		duration := tripFields[tripDurationIndex]

		precipitations, ok := j.precipitationsByDateByCity[city][startDate]
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
