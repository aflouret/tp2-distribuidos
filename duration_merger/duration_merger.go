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
	avgDurationsByDate map[string]average
}

func NewDurationMerger(consumer *middleware.Consumer, producer *middleware.Producer) *DurationMerger {
	avgDurationsByDate := make(map[string]average)

	return &DurationMerger{
		producer:           producer,
		consumer:           consumer,
		avgDurationsByDate: avgDurationsByDate,
	}
}

func (m *DurationMerger) Run() {
	defer m.consumer.Close()
	defer m.producer.Close()

	m.consumer.Consume(m.processMessage)
	m.sendResults()
}

func (m *DurationMerger) processMessage(msg message.Message) {
	if msg.IsEOF() {
		return
	}

	m.mergeResults(msg)
}

func (m *DurationMerger) mergeResults(msg message.Message) error {
	result := msg.Batch[0]
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

	if d, ok := m.avgDurationsByDate[startDate]; ok {
		newAvg := (d.avg*float64(d.count) + avg*float64(count)) / float64(d.count+count)
		d.avg = newAvg
		d.count += count
		m.avgDurationsByDate[startDate] = d
	} else {
		m.avgDurationsByDate[startDate] = average{avg: avg, count: count}
	}
	return nil
}

func (m *DurationMerger) sendResults() {
	sortedDates := make([]string, 0, len(m.avgDurationsByDate))
	for k := range m.avgDurationsByDate {
		sortedDates = append(sortedDates, k)
	}
	sort.Strings(sortedDates)

	result := "Average duration of trips during >30mm precipitation days:\n"
	result += "start_date,average_duration\n"

	for _, date := range sortedDates {
		avg := m.avgDurationsByDate[date].avg
		result += fmt.Sprintf("%s,%v\n", date, avg)
	}

	msg := message.NewTripsBatchMessage("", "", []string{result})
	m.producer.PublishMessage(msg, "")
	eof := message.NewTripsEOFMessage("1")
	m.producer.PublishMessage(eof, "eof")
}
