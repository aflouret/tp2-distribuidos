package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"tp1/common/middleware"
)

const (
	endStationNameIndex = iota
	averageIndex
	countIndex
)

type average struct {
	avg   float64
	count int
}

type DistanceMerger struct {
	producer                  *middleware.Producer
	consumer                  *middleware.Consumer
	minimumDistance           float64
	averageDistancesByStation map[string]average
}

func NewDistanceMerger(consumer *middleware.Consumer, producer *middleware.Producer, minimumDistance float64) *DistanceMerger {
	averageDistancesByStation := make(map[string]average)

	return &DistanceMerger{
		producer:                  producer,
		consumer:                  consumer,
		minimumDistance:           minimumDistance,
		averageDistancesByStation: averageDistancesByStation,
	}
}

func (m *DistanceMerger) Run() {
	defer m.consumer.Close()
	defer m.producer.Close()

	m.consumer.Consume(m.processMessage)
	m.sendResults()
}

func (m *DistanceMerger) processMessage(msg string) {
	if msg == "eof" {
		return
	}

	m.mergeResults(msg)
}

func (m *DistanceMerger) mergeResults(msg string) error {
	fields := strings.Split(msg, ",")
	endStationName := fields[endStationNameIndex]
	avg, err := strconv.ParseFloat(fields[averageIndex], 64)
	if err != nil {
		return err
	}
	count, err := strconv.Atoi(fields[countIndex])
	if err != nil {
		return err
	}

	if d, ok := m.averageDistancesByStation[endStationName]; ok {
		newAvg := (d.avg*float64(d.count) + avg*float64(count)) / float64(d.count+count)
		d.avg = newAvg
		d.count += count
		m.averageDistancesByStation[endStationName] = d
	} else {
		m.averageDistancesByStation[endStationName] = average{avg: avg, count: count}
	}
	return nil
}

func (m *DistanceMerger) sendResults() {
	sortedStations := make([]string, 0, len(m.averageDistancesByStation))
	for k := range m.averageDistancesByStation {
		sortedStations = append(sortedStations, k)
	}
	sort.Strings(sortedStations)

	result := fmt.Sprintf("Stations with more than %v km average to arrive at them:\n", m.minimumDistance)
	result += "end_station_name,average_distance\n"

	for _, s := range sortedStations {
		avg := m.averageDistancesByStation[s].avg
		if avg > m.minimumDistance {
			result += fmt.Sprintf("%s,%v\n", s, avg)
		}
	}
	m.producer.PublishMessage(result, "")
	m.producer.PublishMessage("eof", "eof")
}
