package message

import "strings"

const (
	TripsBatch    = "T"
	StationsBatch = "S"
	WeatherBatch  = "W"
	TripsEOF      = "TE"
	StationsEOF   = "SE"
	WeatherEOF    = "WE"
)

type Message struct {
	MsgType string
	ID      string
	City    string
	Batch   []string
}

func NewBatchMessage(msgType string, id string, city string, trips []string) Message {
	return Message{
		MsgType: msgType,
		ID:      id,
		City:    city,
		Batch:   trips,
	}
}

func NewTripsBatchMessage(id string, city string, trips []string) Message {
	return Message{
		MsgType: TripsBatch,
		ID:      id,
		City:    city,
		Batch:   trips,
	}
}

func NewWeatherBatchMessage(id string, city string, weather []string) Message {
	return Message{
		MsgType: WeatherBatch,
		ID:      id,
		City:    city,
		Batch:   weather,
	}
}

func NewStationsBatchMessage(id string, city string, stations []string) Message {
	return Message{
		MsgType: StationsBatch,
		ID:      id,
		City:    city,
		Batch:   stations,
	}
}

func NewWeatherEOFMessage(id string) Message {
	return Message{
		MsgType: WeatherEOF,
		ID:      id,
	}
}

func NewStationsEOFMessage(id string) Message {
	return Message{
		MsgType: StationsEOF,
		ID:      id,
	}
}

func NewTripsEOFMessage(id string) Message {
	return Message{
		MsgType: TripsEOF,
		ID:      id,
	}
}

func Deserialize(batch string) Message {
	batchFields := strings.Split(batch, "%")
	batchType := strings.Split(batchFields[0], ",")[0]
	id := strings.Split(batchFields[0], ",")[1]
	var city string
	var lines []string
	if batchType == WeatherBatch || batchType == StationsBatch || batchType == TripsBatch {
		city = strings.Split(batchFields[0], ",")[2]
		lines = strings.Split(batchFields[1], ";")
	}
	return Message{
		MsgType: batchType,
		ID:      id,
		City:    city,
		Batch:   lines,
	}
}

func Serialize(m Message) string {
	batch := m.MsgType + "," + m.ID + "," + m.City + "%" + strings.Join(m.Batch, ";")
	return strings.TrimSuffix(batch, ";")
}

func (m Message) IsEOF() bool {
	return m.MsgType == TripsEOF || m.MsgType == WeatherEOF || m.MsgType == StationsEOF
}
