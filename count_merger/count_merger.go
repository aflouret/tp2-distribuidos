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
	yearIndex = iota
	startStationNameIndex
	countIndex
)

type CountMerger struct {
	producer            *middleware.Producer
	consumer            *middleware.Consumer
	year1               string
	year2               string
	countByStationYear1 map[string]map[string]int
	countByStationYear2 map[string]map[string]int
}

func NewCountMerger(consumer *middleware.Consumer, producer *middleware.Producer, year1 string, year2 string) *CountMerger {
	countByStationYear1 := make(map[string]map[string]int)
	countByStationYear2 := make(map[string]map[string]int)
	return &CountMerger{
		producer:            producer,
		consumer:            consumer,
		year1:               year1,
		year2:               year2,
		countByStationYear1: countByStationYear1,
		countByStationYear2: countByStationYear2,
	}
}

func (m *CountMerger) Run() error {
	err := m.consumer.Consume(m.processMessage)
	if err != nil {
		return err
	}
	err = m.producer.Close()
	if err != nil {
		return err
	}
	err = m.consumer.Close()
	if err != nil {
		return err
	}
	return nil
}

func (m *CountMerger) processMessage(msg message.Message) error {
	if msg.IsEOF() {
		if msg.MsgType == message.ClientEOF {
			if msg.ClientID == message.AllClients {
				err := m.producer.PublishMessage(msg, message.AllClients)
				if err != nil {
					return err
				}
				m.countByStationYear1 = make(map[string]map[string]int)
				m.countByStationYear2 = make(map[string]map[string]int)
			} else {
				delete(m.countByStationYear1, msg.ClientID)
				delete(m.countByStationYear2, msg.ClientID)
			}
			return nil
		}
		return m.sendResults(msg.ClientID)
	}

	err := m.mergeResults(msg)
	if err != nil {
		return err
	}
	return nil
}

func (m *CountMerger) mergeResults(msg message.Message) error {
	countByStationYear1, ok := m.countByStationYear1[msg.ClientID]
	if !ok {
		countByStationYear1 = make(map[string]int)
	}
	countByStationYear2, ok := m.countByStationYear2[msg.ClientID]
	if !ok {
		countByStationYear2 = make(map[string]int)
	}

	results := msg.Batch
	for _, result := range results {
		fields := strings.Split(result, ",")
		year := fields[yearIndex]
		startStationName := fields[startStationNameIndex]
		count, err := strconv.Atoi(fields[countIndex])
		if err != nil {
			return err
		}

		if year == m.year1 {
			if c, ok := countByStationYear1[startStationName]; ok {
				countByStationYear1[startStationName] = c + count
			} else {
				countByStationYear1[startStationName] = count
			}
		} else if year == m.year2 {
			if c, ok := countByStationYear2[startStationName]; ok {
				countByStationYear2[startStationName] = c + count
			} else {
				countByStationYear2[startStationName] = count
			}
		}
	}

	m.countByStationYear1[msg.ClientID] = countByStationYear1
	m.countByStationYear2[msg.ClientID] = countByStationYear2

	return nil
}

func (m *CountMerger) sendResults(clientID string) error {
	sortedStations := make([]string, 0, len(m.countByStationYear2[clientID]))
	for k := range m.countByStationYear2[clientID] {
		sortedStations = append(sortedStations, k)
	}
	sort.Strings(sortedStations)

	result := fmt.Sprintf("Stations that doubled the number of trips between %s and %s:\n", m.year1, m.year2)
	result += fmt.Sprintf("start_station_name,trips_count_%s,trips_count_%s\n", m.year2, m.year1)

	for _, s := range sortedStations {
		countYear2 := m.countByStationYear2[clientID][s]
		if countYear1, ok := m.countByStationYear1[clientID][s]; ok {
			if countYear2 > 2*countYear1 {
				result += fmt.Sprintf("%s,%v,%v\n", s, countYear2, countYear1)
			}
		}
	}

	msg := message.NewResultsBatchMessage("count_merger", clientID, []string{result})
	err := m.producer.PublishMessage(msg, msg.ClientID)
	if err != nil {
		return err
	}
	eof := message.NewResultsEOFMessage(clientID)
	return m.producer.PublishMessage(eof, msg.ClientID)
}
