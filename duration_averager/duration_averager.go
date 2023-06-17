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
	startDateIndex = iota
	durationIndex
)

type average struct {
	avg   float64
	count int
}

type DurationAverager struct {
	producer           *middleware.Producer
	consumer           *middleware.Consumer
	avgDurationsByDate map[string]average
	msgCount           int
	startTime          time.Time
}

func NewDurationAverager(consumer *middleware.Consumer, producer *middleware.Producer) *DurationAverager {
	avgDurationsByDate := make(map[string]average)

	return &DurationAverager{
		producer:           producer,
		consumer:           consumer,
		avgDurationsByDate: avgDurationsByDate,
	}
}

func (a *DurationAverager) Run() {
	defer a.consumer.Close()
	defer a.producer.Close()

	a.startTime = time.Now()
	a.consumer.Consume(a.processMessage)
}

func (a *DurationAverager) processMessage(msg message.Message) {
	if msg.IsEOF() {
		a.sendResults()
		return
	}

	trips := msg.Batch

	a.updateAverage(trips)

	if a.msgCount%20000 == 0 {
		fmt.Printf("Time: %s Received batch %v\n", time.Since(a.startTime).String(), msg.ID)
	}
	a.msgCount++
}

func (a *DurationAverager) updateAverage(trips []string) {
	for _, trip := range trips {
		fields := strings.Split(trip, ",")
		startDate := fields[startDateIndex]
		duration, err := strconv.ParseFloat(fields[durationIndex], 64)
		if err != nil {
			fmt.Println(fmt.Errorf("error parsing duration: %w", err))
			continue
		}

		if d, ok := a.avgDurationsByDate[startDate]; ok {
			newAvg := (d.avg*float64(d.count) + duration) / float64(d.count+1)
			d.avg = newAvg
			d.count++
			a.avgDurationsByDate[startDate] = d
		} else {
			a.avgDurationsByDate[startDate] = average{avg: duration, count: 1}
		}
	}
}

func (a *DurationAverager) sendResults() {
	for k, v := range a.avgDurationsByDate {
		result := fmt.Sprintf("%s,%v,%v", k, v.avg, v.count)
		msg := message.NewTripsBatchMessage("", "", []string{result})
		a.producer.PublishMessage(msg, "")
	}
	eof := message.NewTripsEOFMessage("1")
	a.producer.PublishMessage(eof, "")
}
