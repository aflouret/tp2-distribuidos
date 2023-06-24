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
	startDateIndex = iota
	durationIndex
)
const batchSize = 500

type average struct {
	avg   float64
	count int
}

type DurationAverager struct {
	instanceID         string
	producer           *middleware.Producer
	consumer           *middleware.Consumer
	avgDurationsByDate map[string]map[string]average
	msgCount           int
	startTime          time.Time
}

func NewDurationAverager(instanceID string, consumer *middleware.Consumer, producer *middleware.Producer) *DurationAverager {
	avgDurationsByDate := make(map[string]map[string]average)

	return &DurationAverager{
		instanceID:         instanceID,
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
		a.sendResults(msg.ClientID)
		delete(a.avgDurationsByDate, msg.ClientID)
		return
	}

	a.updateAverage(msg)

	if a.msgCount%20000 == 0 {
		fmt.Printf("[Client %s] Time: %s Received batch %v\n", msg.ClientID, time.Since(a.startTime).String(), msg.ID)
	}
	a.msgCount++
}

func (a *DurationAverager) updateAverage(msg message.Message) {
	trips := msg.Batch
	avgDurationsByDate, ok := a.avgDurationsByDate[msg.ClientID]
	if !ok {
		avgDurationsByDate = make(map[string]average)
	}
	for _, trip := range trips {
		fields := strings.Split(trip, ",")
		startDate := fields[startDateIndex]
		duration, err := strconv.ParseFloat(fields[durationIndex], 64)
		if err != nil {
			fmt.Println(fmt.Errorf("error parsing duration: %w", err))
			continue
		}

		if d, ok := avgDurationsByDate[startDate]; ok {
			newAvg := (d.avg*float64(d.count) + duration) / float64(d.count+1)
			d.avg = newAvg
			d.count++
			avgDurationsByDate[startDate] = d
		} else {
			avgDurationsByDate[startDate] = average{avg: duration, count: 1}
		}
	}
	a.avgDurationsByDate[msg.ClientID] = avgDurationsByDate
}

func (a *DurationAverager) sendResults(clientID string) {
	sortedDates := make([]string, 0, len(a.avgDurationsByDate[clientID]))
	for k := range a.avgDurationsByDate[clientID] {
		sortedDates = append(sortedDates, k)
	}
	sort.Strings(sortedDates)

	batch := make([]string, 0, batchSize)
	batchNumber := 1
	for i, s := range sortedDates {
		index := i + 1
		value := a.avgDurationsByDate[clientID][s]
		result := fmt.Sprintf("%s,%v,%v", s, value.avg, value.count)
		batch = append(batch, result)
		if index%batchSize == 0 || index == len(sortedDates) {
			msg := message.NewTripsBatchMessage(clientID+"."+a.instanceID+"."+strconv.Itoa(batchNumber), clientID, "", batch)
			a.producer.PublishMessage(msg, "")
			batch = make([]string, 0, batchSize)
			batchNumber++
		}
	}
	eof := message.NewTripsEOFMessage("1", clientID)
	a.producer.PublishMessage(eof, "")
}
