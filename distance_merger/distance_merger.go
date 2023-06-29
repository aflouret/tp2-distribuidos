package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"tp1/common/message"
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
	producer              *middleware.Producer
	consumer              *middleware.Consumer
	minimumDistance       float64
	avgDistancesByStation map[string]map[string]average
}

func NewDistanceMerger(consumer *middleware.Consumer, producer *middleware.Producer, minimumDistance float64) *DistanceMerger {
	avgDistancesByStation := make(map[string]map[string]average)

	return &DistanceMerger{
		producer:              producer,
		consumer:              consumer,
		minimumDistance:       minimumDistance,
		avgDistancesByStation: avgDistancesByStation,
	}
}

func (m *DistanceMerger) Run() error {
	err := m.consumer.Consume(m.processMessage)
	if err != nil {
		return err
	}
	err = m.consumer.Close()
	if err != nil {
		return err
	}
	err = m.producer.Close()
	if err != nil {
		return err
	}
	return nil
}

func (m *DistanceMerger) processMessage(msg message.Message) error {
	if msg.IsEOF() {
		if msg.MsgType == message.ClientEOF {
			if msg.ClientID == message.AllClients {
				err := m.producer.PublishMessage(msg, message.AllClients)
				if err != nil {
					return err
				}
				m.avgDistancesByStation = make(map[string]map[string]average)
			} else {
				delete(m.avgDistancesByStation, msg.ClientID)
			}
			return nil
		}
		return m.sendResults(msg.ClientID)
	}

	return m.mergeResults(msg)

}

func (m *DistanceMerger) mergeResults(msg message.Message) error {
	avgDistancesByStation, ok := m.avgDistancesByStation[msg.ClientID]
	if !ok {
		avgDistancesByStation = make(map[string]average)
	}

	results := msg.Batch
	for _, result := range results {
		fields := strings.Split(result, ",")
		endStationName := fields[endStationNameIndex]
		avg, err := strconv.ParseFloat(fields[averageIndex], 64)
		if err != nil {
			return err
		}
		count, err := strconv.Atoi(fields[countIndex])
		if err != nil {
			return err
		}

		if d, ok := avgDistancesByStation[endStationName]; ok {
			newAvg := (d.avg*float64(d.count) + avg*float64(count)) / float64(d.count+count)
			d.avg = newAvg
			d.count += count
			avgDistancesByStation[endStationName] = d
		} else {
			avgDistancesByStation[endStationName] = average{avg: avg, count: count}
		}
	}

	m.avgDistancesByStation[msg.ClientID] = avgDistancesByStation
	return nil
}

func (m *DistanceMerger) sendResults(clientID string) error {
	sortedStations := make([]string, 0, len(m.avgDistancesByStation[clientID]))
	for k := range m.avgDistancesByStation[clientID] {
		sortedStations = append(sortedStations, k)
	}
	sort.Strings(sortedStations)

	result := fmt.Sprintf("Stations with more than %v km average to arrive at them:\n", 0)
	result += "end_station_name,average_distance\n"

	for _, s := range sortedStations {
		avg := m.avgDistancesByStation[clientID][s].avg
		result += fmt.Sprintf("%s,%.6f\n", s, avg)
	}
	msg := message.NewResultsBatchMessage("distance_merger", clientID, []string{result})
	err := m.producer.PublishMessage(msg, msg.ClientID)
	if err != nil {
		return err
	}
	eof := message.NewResultsEOFMessage(clientID)
	return m.producer.PublishMessage(eof, msg.ClientID)
}
