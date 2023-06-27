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

func (d *DataDropper) processMessage(msg message.Message) {

	switch msg.MsgType {
	case message.StationsBatch, message.StationsEOF:
		d.processStationsMessage(msg)
	case message.WeatherBatch, message.WeatherEOF:
		d.processWeatherMessage(msg)
	case message.TripsBatch, message.TripsEOF:
		d.processTripsMessage(msg)
	case message.ClientEOF:
		d.processClientEOFMessage(msg)
	}
}
func (d *DataDropper) processClientEOFMessage(msg message.Message) {
	d.stationsJoinerProducer.PublishMessage(msg, "")
	d.weatherJoinerProducer.PublishMessage(msg, "")
}
func (d *DataDropper) processWeatherMessage(msg message.Message) {
	d.weatherJoinerProducer.PublishMessage(msg, "weather")
}

func (d *DataDropper) processStationsMessage(msg message.Message) {
	d.stationsJoinerProducer.PublishMessage(msg, "stations")
}

func (d *DataDropper) processTripsMessage(msg message.Message) {
	if msg.IsEOF() {
		d.stationsJoinerProducer.PublishMessage(msg, "")
		d.weatherJoinerProducer.PublishMessage(msg, "")
		return
	}

	if d.msgCount%5000 == 0 {
		fmt.Printf("[Client %s] Time: %s Received batch %v\n", msg.ClientID, time.Since(d.startTime).String(), msg.ID)
	}

	weatherJoinerTrips, stationsJoinerTrips := d.dropData(msg)

	weatherJoinerMessage := message.NewTripsBatchMessage(msg.ID, msg.ClientID, msg.City, weatherJoinerTrips)
	d.weatherJoinerProducer.PublishMessage(weatherJoinerMessage, "")

	stationsJoinerMessage := message.NewTripsBatchMessage(msg.ID, msg.ClientID, msg.City, stationsJoinerTrips)
	d.stationsJoinerProducer.PublishMessage(stationsJoinerMessage, "")

	d.msgCount++
}

func (d *DataDropper) dropData(msg message.Message) ([]string, []string) {
	trips := msg.Batch
	tripsToSendToWeatherJoiner := make([]string, len(trips))
	tripsToSendToStationsJoiner := make([]string, len(trips))
	fieldsToSendToWeatherJoiner := make([]string, len(columnsForWeatherJoiner))
	fieldsToSendToStationsJoiner := make([]string, len(columnsForStationsJoiner))

	for i := range trips {
		fields := strings.Split(trips[i], ",")
		duration, err := strconv.ParseFloat(fields[durationSecIndex], 64)
		if err != nil || duration < 0 {
			fields[durationSecIndex] = "0"
		}
		day := strings.Split(fields[startDateIndex], " ")[0]
		fields[startDateIndex] = day

		for i, col := range columnsForWeatherJoiner {
			fieldsToSendToWeatherJoiner[i] = fields[col]
		}
		tripsToSendToWeatherJoiner[i] = strings.Join(fieldsToSendToWeatherJoiner, ",")

		for i, col := range columnsForStationsJoiner {
			fieldsToSendToStationsJoiner[i] = fields[col]
		}
		tripsToSendToStationsJoiner[i] = strings.Join(fieldsToSendToStationsJoiner, ",")
	}
	return tripsToSendToWeatherJoiner, tripsToSendToStationsJoiner
}
