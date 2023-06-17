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
	endStationNameIndex = iota
	distanceIndex
)

type average struct {
	avg   float64
	count int
}

type DistanceAverager struct {
	producer              *middleware.Producer
	consumer              *middleware.Consumer
	avgDistancesByStation map[string]average
	msgCount              int
	startTime             time.Time
}

func NewDistanceAverager(consumer *middleware.Consumer, producer *middleware.Producer) *DistanceAverager {
	avgDistancesByStation := make(map[string]average)

	return &DistanceAverager{
		producer:              producer,
		consumer:              consumer,
		avgDistancesByStation: avgDistancesByStation,
	}
}

func (a *DistanceAverager) Run() {
	defer a.consumer.Close()
	defer a.producer.Close()

	a.startTime = time.Now()
	a.consumer.Consume(a.processMessage)
	a.sendResults()
}

func (a *DistanceAverager) processMessage(msg message.Message) {
	if msg.IsEOF() {
		return
	}

	trips := msg.Batch

	a.updateAverage(trips)

	if a.msgCount%20000 == 0 {
		fmt.Printf("Time: %s Received batch %v\n", time.Since(a.startTime).String(), msg.ID)
	}
	a.msgCount++
}

func (a *DistanceAverager) updateAverage(trips []string) {
	for _, trip := range trips {
		fields := strings.Split(trip, ",")
		endStationName := fields[endStationNameIndex]
		distance, err := strconv.ParseFloat(fields[distanceIndex], 64)
		if err != nil {
			fmt.Println(fmt.Errorf("error parsing distance: %w", err))
			continue
		}

		if d, ok := a.avgDistancesByStation[endStationName]; ok {
			newAvg := (d.avg*float64(d.count) + distance) / float64(d.count+1)
			d.avg = newAvg
			d.count++
			a.avgDistancesByStation[endStationName] = d
		} else {
			a.avgDistancesByStation[endStationName] = average{avg: distance, count: 1}
		}
	}
}

func (a *DistanceAverager) sendResults() {
	for k, v := range a.avgDistancesByStation {
		result := fmt.Sprintf("%s,%v,%v", k, v.avg, v.count)
		msg := message.NewTripsBatchMessage("", "", []string{result})
		a.producer.PublishMessage(msg, "")
	}
	eof := message.NewTripsEOFMessage("1")
	a.producer.PublishMessage(eof, "")
}
