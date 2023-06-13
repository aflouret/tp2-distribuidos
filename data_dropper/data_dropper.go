package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"tp1/common/middleware"
	"tp1/common/utils"
)

const (
	startDateIndex        = 0
	startStationCodeIndex = 1
	endStationCodeIndex   = 3
	durationSecIndex      = 4
	yearIdIndex           = 6
)

var columnsForWeatherJoiner = []int{startDateIndex, durationSecIndex}
var columnsForStationsJoiner = []int{startStationCodeIndex, endStationCodeIndex, yearIdIndex}

type DataDropper struct {
	stationsJoinerProducer *middleware.Producer
	weatherJoinerProducer  *middleware.Producer
	consumer               *middleware.Consumer
	msgCount               int
	startTime              time.Time
}

func NewDataDropper(consumer *middleware.Consumer, stationsJoinerProducer *middleware.Producer, weatherJoinerProducer *middleware.Producer) *DataDropper {
	return &DataDropper{
		stationsJoinerProducer: stationsJoinerProducer,
		weatherJoinerProducer:  weatherJoinerProducer,
		consumer:               consumer,
		startTime:              time.Now(),
	}
}

func (d *DataDropper) Run() {
	defer d.consumer.Close()
	defer d.stationsJoinerProducer.Close()
	defer d.weatherJoinerProducer.Close()

	d.consumer.Consume(d.processMessage)
}

func (d *DataDropper) processMessage(msg string) {
	if msg == "eof" {
		d.stationsJoinerProducer.PublishMessage(msg, "")
		d.weatherJoinerProducer.PublishMessage(msg, "")
		return
	}

	id, city, trips := utils.ParseBatch(msg)
	sanitizedTrips := d.sanitize(trips)

	weatherJoinerTrips := d.dropDataForWeatherJoiner(sanitizedTrips)
	weatherJoinerBatch := utils.CreateBatch(id, city, weatherJoinerTrips)
	d.weatherJoinerProducer.PublishMessage(weatherJoinerBatch, "")

	stationsJoinerTrips := d.dropDataForStationsJoiner(sanitizedTrips)
	stationsJoinerBatch := utils.CreateBatch(id, city, stationsJoinerTrips)
	d.stationsJoinerProducer.PublishMessage(stationsJoinerBatch, "")

	if d.msgCount%20000 == 0 {
		fmt.Printf("Time: %s Received batch %v\n", time.Since(d.startTime).String(), id)
	}
	d.msgCount++
}

func (d *DataDropper) sanitize(trips []string) []string {
	sanitizedTrips := make([]string, 0, len(trips))
	for i := range trips {
		fields := strings.Split(trips[i], ",")
		duration, err := strconv.ParseFloat(fields[durationSecIndex], 64)
		if err != nil || duration < 0 {
			fields[durationSecIndex] = "0"
		}

		day := strings.Split(fields[startDateIndex], " ")[0]
		fields[startDateIndex] = day
		sanitizedTrips = append(sanitizedTrips, strings.Join(fields, ","))
	}
	return sanitizedTrips
}

func (d *DataDropper) dropDataForWeatherJoiner(trips []string) []string {
	tripsToSend := make([]string, 0, len(trips))
	for i := range trips {
		fields := strings.Split(trips[i], ",")
		var fieldsToSend []string
		for _, col := range columnsForWeatherJoiner {
			fieldsToSend = append(fieldsToSend, fields[col])
		}
		tripsToSend = append(tripsToSend, strings.Join(fieldsToSend, ","))
	}
	return tripsToSend
}

func (d *DataDropper) dropDataForStationsJoiner(trips []string) []string {
	tripsToSend := make([]string, 0, len(trips))
	for i := range trips {
		fields := strings.Split(trips[i], ",")
		var fieldsToSend []string
		for _, col := range columnsForStationsJoiner {
			fieldsToSend = append(fieldsToSend, fields[col])
		}
		tripsToSend = append(tripsToSend, strings.Join(fieldsToSend, ","))
	}
	return tripsToSend
}
