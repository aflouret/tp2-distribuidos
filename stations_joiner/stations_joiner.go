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
	tripStartStationCodeIndex = iota
	tripEndStationCodeIndex
	tripYearIdIndex
)
const batchSize = 500

type station struct {
	name      string
	latitude  string
	longitude string
}

type StationsJoiner struct {
	instanceID                 string
	yearFilterProducer         *middleware.Producer
	distanceCalculatorProducer *middleware.Producer
	consumer                   *middleware.Consumer
	stations                   map[string]map[string]station
	pendingTrips               map[string]map[string][]string
	stationsEOFs               map[string]bool
	msgCount                   int
	startTime                  time.Time
}

func NewStationsJoiner(
	instanceID string,
	consumer *middleware.Consumer,
	yearFilterProducer *middleware.Producer,
	distanceCalculatorProducer *middleware.Producer,
) *StationsJoiner {
	stations := make(map[string]map[string]station)
	pendingTrips := make(map[string]map[string][]string)
	stationsEOFs := make(map[string]bool)
	return &StationsJoiner{
		instanceID:                 instanceID,
		consumer:                   consumer,
		yearFilterProducer:         yearFilterProducer,
		distanceCalculatorProducer: distanceCalculatorProducer,
		stations:                   stations,
		pendingTrips:               pendingTrips,
		stationsEOFs:               stationsEOFs,
		startTime:                  time.Now(),
	}
}

func (j *StationsJoiner) Run() {
	defer j.yearFilterProducer.Close()
	defer j.distanceCalculatorProducer.Close()

	j.consumer.Consume(j.processMessage)
	j.consumer.Close()
}

func (j *StationsJoiner) processMessage(msg message.Message) {
	switch msg.MsgType {
	case message.StationsBatch, message.StationsEOF:
		j.processStationsMessage(msg)
	case message.TripsBatch, message.TripsEOF:
		j.processTripsMessage(msg)
	}
}

func (j *StationsJoiner) processStationsMessage(msg message.Message) {
	if msg.IsEOF() {
		j.stationsEOFs[msg.ClientID] = true
		j.processPendingTrips(msg.ClientID)
		return
	}

	if _, ok := j.stations[msg.ClientID]; !ok {
		j.stations[msg.ClientID] = make(map[string]station)
	}

	stations := msg.Batch
	for _, s := range stations {
		fields := strings.Split(s, ",")
		code := fields[0]
		name := fields[1]
		latitude := fields[2]
		longitude := fields[3]
		year := fields[4]
		key := getStationKey(code, year, msg.City)
		j.stations[msg.ClientID][key] = station{name, latitude, longitude}
	}
}

func (j *StationsJoiner) processTripsMessage(msg message.Message) {
	if msg.IsEOF() {
		j.yearFilterProducer.PublishMessage(msg, "")
		j.distanceCalculatorProducer.PublishMessage(msg, "")
		delete(j.stations, msg.ClientID)
		return
	}
	if j.msgCount%5000 == 0 {
		fmt.Printf("[Client %s] Time: %s Received batch %v\n", msg.ClientID, time.Since(j.startTime).String(), msg.ID)
	}
	trips := msg.Batch
	joinedTrips := j.joinStations(msg.City, trips, msg.ClientID)

	if len(joinedTrips) > 0 {

		yearFilterTrips := j.dropDataForYearFilter(joinedTrips)

		yearFilterBatch := message.NewTripsBatchMessage(msg.ID, msg.ClientID, "", yearFilterTrips)

		j.yearFilterProducer.PublishMessage(yearFilterBatch, "")

		if msg.City == "montreal" {
			distanceCalculatorBatch := message.NewTripsBatchMessage(msg.ID, msg.ClientID, "", joinedTrips)
			j.distanceCalculatorProducer.PublishMessage(distanceCalculatorBatch, "")
		}
	}

	j.msgCount++

}

func getStationKey(code, year, city string) string {
	return fmt.Sprintf("%s-%s-%s", code, year, city)
}

func (j *StationsJoiner) joinStations(city string, trips []string, clientID string) []string {
	stations := j.stations[clientID]
	joinedTrips := make([]string, len(trips))
	i := 0
	for _, trip := range trips {
		tripFields := strings.Split(trip, ",")

		startStationCode := tripFields[tripStartStationCodeIndex]
		endStationCode := tripFields[tripEndStationCodeIndex]
		year := tripFields[tripYearIdIndex]
		startStationKey := getStationKey(startStationCode, year, city)
		startStation, ok := stations[startStationKey]
		if !ok {
			if !j.receivedStationsEOF(clientID) {
				j.savePendingTrip(clientID, city, trip)
			}
			continue
		}
		endStationKey := getStationKey(endStationCode, year, city)
		endStation, ok := stations[endStationKey]
		if !ok {
			if !j.receivedStationsEOF(clientID) {
				j.savePendingTrip(clientID, city, trip)
			}
			continue
		}
		joinedTrip := fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s",
			startStation.name,
			startStation.latitude,
			startStation.longitude,
			endStation.name,
			endStation.latitude,
			endStation.longitude,
			year,
		)
		joinedTrips[i] = joinedTrip
		i++
	}
	return joinedTrips[:i]
}

func (j *StationsJoiner) dropDataForYearFilter(trips []string) []string {
	tripsToSend := make([]string, len(trips))
	for i, trip := range trips {
		fields := strings.Split(trip, ",")

		startStationName := fields[0]
		year := fields[6]

		tripToSend := fmt.Sprintf("%s,%s", startStationName, year)
		tripsToSend[i] = tripToSend
	}
	return tripsToSend
}

func (j *StationsJoiner) receivedStationsEOF(clientID string) bool {
	return j.stationsEOFs[clientID]
}

func (j *StationsJoiner) savePendingTrip(clientID string, city string, trip string) {
	if _, ok := j.pendingTrips[clientID]; !ok {
		j.pendingTrips[clientID] = make(map[string][]string)
	}
	if _, ok := j.pendingTrips[clientID][city]; !ok {
		j.pendingTrips[clientID][city] = make([]string, batchSize)
	}
	j.pendingTrips[clientID][city] = append(j.pendingTrips[clientID][city], trip)
}

func (j *StationsJoiner) processPendingTrips(clientID string) {
	tripsByCity, ok := j.pendingTrips[clientID]
	if !ok {
		return
	}
	for city, trips := range tripsByCity {
		fmt.Printf("Processing %v pending trips\n", len(trips))
		batch := make([]string, 0, batchSize)
		batchNumber := 1
		for i, trip := range trips {
			index := i + 1
			batch = append(batch, trip)
			if index%batchSize == 0 || index == len(trips) {
				msg := message.NewTripsBatchMessage("s"+"."+j.instanceID+"."+strconv.Itoa(batchNumber), clientID, city, batch)
				j.processTripsMessage(msg)
				batch = make([]string, 0, batchSize)
				batchNumber++
			}
		}
	}

}
