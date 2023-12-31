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
	startDateIndex = iota
	averageIndex
	countIndex
)

type average struct {
	avg   float64
	count int
}

type DurationMerger struct {
	producer           *middleware.Producer
	consumer           *middleware.Consumer
	avgDurationsByDate map[string]map[string]average
}

func NewDurationMerger(consumer *middleware.Consumer, producer *middleware.Producer) *DurationMerger {
	avgDurationsByDate := make(map[string]map[string]average)

	return &DurationMerger{
		producer:           producer,
		consumer:           consumer,
		avgDurationsByDate: avgDurationsByDate,
	}
}

func (m *DurationMerger) Run() error {
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

func (m *DurationMerger) processMessage(msg message.Message) error {
	if msg.IsEOF() {
		if msg.MsgType == message.ClientEOF {
			if msg.ClientID == message.AllClients {
				err := m.producer.PublishMessage(msg, message.AllClients)
				if err != nil {
					return err
				}
				m.avgDurationsByDate = make(map[string]map[string]average)
			} else {
				delete(m.avgDurationsByDate, msg.ClientID)
			}
			return nil
		}
		return m.sendResults(msg.ClientID)
	}

	return m.mergeResults(msg)
}

func (m *DurationMerger) mergeResults(msg message.Message) error {
	avgDurationsByDate, ok := m.avgDurationsByDate[msg.ClientID]
	if !ok {
		avgDurationsByDate = make(map[string]average)
	}

	results := msg.Batch
	for _, result := range results {
		fields := strings.Split(result, ",")
		startDate := fields[startDateIndex]
		avg, err := strconv.ParseFloat(fields[averageIndex], 64)
		if err != nil {
			return err
		}
		count, err := strconv.Atoi(fields[countIndex])
		if err != nil {
			return err
		}

		if d, ok := avgDurationsByDate[startDate]; ok {
			newAvg := (d.avg*float64(d.count) + avg*float64(count)) / float64(d.count+count)
			d.avg = newAvg
			d.count += count
			avgDurationsByDate[startDate] = d
		} else {
			avgDurationsByDate[startDate] = average{avg: avg, count: count}
		}
	}
	m.avgDurationsByDate[msg.ClientID] = avgDurationsByDate
	return nil
}

func (m *DurationMerger) sendResults(clientID string) error {
	sortedDates := make([]string, 0, len(m.avgDurationsByDate[clientID]))
	for k := range m.avgDurationsByDate[clientID] {
		sortedDates = append(sortedDates, k)
	}
	sort.Strings(sortedDates)

	result := "Average duration of trips during >30mm precipitation days:\n"
	result += "start_date,average_duration\n"

	for _, date := range sortedDates {
		avg := m.avgDurationsByDate[clientID][date].avg
		result += fmt.Sprintf("%s,%.6f\n", date, avg)
	}

	msg := message.NewResultsBatchMessage("0", clientID, []string{result})
	err := m.producer.PublishMessage(msg, msg.ClientID)
	if err != nil {
		return err
	}
	eof := message.NewResultsEOFMessage(clientID)
	return m.producer.PublishMessage(eof, msg.ClientID)
}
