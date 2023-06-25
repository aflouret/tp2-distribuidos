package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"tp1/common/message"
	"tp1/common/middleware"
)

const (
	endStationNameIndex = iota
	distanceIndex
)
const batchSize = 500

type average struct {
	avg   float64
	count int
}

type DistanceAverager struct {
	instanceID            string
	producer              *middleware.Producer
	consumer              *middleware.Consumer
	avgDistancesByStation map[string]map[string]average
	msgCount              int
	startTime             time.Time
}

func NewDistanceAverager(instanceID string, consumer *middleware.Consumer, producer *middleware.Producer) *DistanceAverager {
	avgDistancesByStation := make(map[string]map[string]average)

	return &DistanceAverager{
		instanceID:            instanceID,
		producer:              producer,
		consumer:              consumer,
		avgDistancesByStation: avgDistancesByStation,
	}
}

func (a *DistanceAverager) Run() {
	defer a.consumer.Close()
	defer a.producer.Close()

	a.startTime = time.Now()
	a.consumer.ConsumeAndFilterDuplicates(a.processMessage)
}

func (a *DistanceAverager) processMessage(msg message.Message) {
	if msg.IsEOF() {
		a.sendResults(msg.ClientID)
		delete(a.avgDistancesByStation, msg.ClientID)
		return
	}

	a.updateAverage(msg)

	if a.msgCount%20000 == 0 {
		fmt.Printf("[Client %s] Time: %s Received batch %v\n", msg.ClientID, time.Since(a.startTime).String(), msg.ID)
	}
	a.msgCount++
}

func (a *DistanceAverager) updateAverage(msg message.Message) {
	trips := msg.Batch
	avgDistancesByStation, ok := a.avgDistancesByStation[msg.ClientID]
	if !ok {
		avgDistancesByStation = make(map[string]average)
	}
	for _, trip := range trips {
		fields := strings.Split(trip, ",")
		endStationName := fields[endStationNameIndex]
		distance, err := strconv.ParseFloat(fields[distanceIndex], 64)
		if err != nil {
			fmt.Println(fmt.Errorf("error parsing distance: %w", err))
			continue
		}

		if d, ok := avgDistancesByStation[endStationName]; ok {
			newAvg := (d.avg*float64(d.count) + distance) / float64(d.count+1)
			d.avg = newAvg
			d.count++
			avgDistancesByStation[endStationName] = d
		} else {
			avgDistancesByStation[endStationName] = average{avg: distance, count: 1}
		}
	}
	a.avgDistancesByStation[msg.ClientID] = avgDistancesByStation
}

func (a *DistanceAverager) sendResults(clientID string) {
	sortedStations := make([]string, 0, len(a.avgDistancesByStation[clientID]))
	for k := range a.avgDistancesByStation[clientID] {
		sortedStations = append(sortedStations, k)
	}
	sort.Strings(sortedStations)

	batch := make([]string, 0, batchSize)
	batchNumber := 1
	for i, s := range sortedStations {
		index := i + 1
		value := a.avgDistancesByStation[clientID][s]
		result := fmt.Sprintf("%s,%v,%v", s, value.avg, value.count)
		batch = append(batch, result)
		if index%batchSize == 0 || index == len(sortedStations) {
			msg := message.NewTripsBatchMessage(clientID+"."+a.instanceID+"."+strconv.Itoa(batchNumber), clientID, "", batch)
			a.producer.PublishMessage(msg, "")
			batch = make([]string, 0, batchSize)
			batchNumber++
		}
	}
	eof := message.NewTripsEOFMessage("1", clientID)
	a.producer.PublishMessage(eof, "")
}
