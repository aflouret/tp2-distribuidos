package message

import "strings"

const (
	TripsBatch    = "T"
	StationsBatch = "S"
	WeatherBatch  = "W"
	ResultsBatch  = "R"
	TripsEOF      = "TE"
	StationsEOF   = "SE"
	WeatherEOF    = "WE"
	ResultsEOF    = "RE"
	ClientEOF     = "CE"
)

const AllClients = "all"

type Message struct {
	MsgType  string
	ID       string
	ClientID string
	City     string
	Batch    []string
}

func NewBatchMessage(msgType string, id string, clientID string, city string, trips []string) Message {
	return Message{
		MsgType:  msgType,
		ID:       id,
		ClientID: clientID,
		City:     city,
		Batch:    trips,
	}
}

func NewTripsBatchMessage(id string, clientID string, city string, trips []string) Message {
	return Message{
		MsgType:  TripsBatch,
		ID:       id,
		ClientID: clientID,
		City:     city,
		Batch:    trips,
	}
}

func NewResultsBatchMessage(id string, clientID string, results []string) Message {
	return Message{
		MsgType:  ResultsBatch,
		ID:       id,
		ClientID: clientID,
		Batch:    results,
	}
}

func NewWeatherEOFMessage(clientID string) Message {
	return Message{
		MsgType:  WeatherEOF,
		ClientID: clientID,
	}
}

func NewStationsEOFMessage(clientID string) Message {
	return Message{
		MsgType:  StationsEOF,
		ClientID: clientID,
	}
}

func NewTripsEOFMessage(clientID string) Message {
	return Message{
		MsgType:  TripsEOF,
		ClientID: clientID,
	}
}

func NewResultsEOFMessage(clientID string) Message {
	return Message{
		MsgType:  ResultsEOF,
		ClientID: clientID,
	}
}

func NewClientEOFMessage(clientID string) Message {
	return Message{
		MsgType:  ClientEOF,
		ClientID: clientID,
	}
}

func Deserialize(batch string) Message {
	batchFields := strings.Split(batch, "%")
	batchType := strings.Split(batchFields[0], ",")[0]
	id := strings.Split(batchFields[0], ",")[1]
	clientID := strings.Split(batchFields[0], ",")[2]
	var city string
	var lines []string
	if batchType == WeatherBatch || batchType == StationsBatch || batchType == TripsBatch || batchType == ResultsBatch {
		city = strings.Split(batchFields[0], ",")[3]
		lines = strings.Split(batchFields[1], ";")
	}
	return Message{
		MsgType:  batchType,
		ID:       id,
		ClientID: clientID,
		City:     city,
		Batch:    lines,
	}
}

func Serialize(m Message) string {
	batch := m.MsgType + "," + m.ID + "," + m.ClientID + "," + m.City + "%" + strings.Join(m.Batch, ";")
	return strings.TrimSuffix(batch, ";")
}

func (m Message) IsEOF() bool {
	return m.MsgType == TripsEOF || m.MsgType == WeatherEOF || m.MsgType == StationsEOF || m.MsgType == ResultsEOF || m.MsgType == ClientEOF
}
