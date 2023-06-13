package protocol

import (
	"encoding/binary"
	"net"
)

const (
	Data = iota
	Ack
	BeginStations
	EndStations
	BeginWeather
	EndWeather
	EndStaticData
	BeginTrips
	EndTrips
	GetResults
)

type Message struct {
	Type    uint8
	Payload string
}

func NewDataMessage(data string) Message {
	return Message{
		Type:    Data,
		Payload: data,
	}
}

func NewControlMessage(msgType uint8, payload string) Message {
	return Message{
		Type:    msgType,
		Payload: payload,
	}
}

func Send(conn net.Conn, msg Message) error {
	payload := []byte(msg.Payload)
	length := len(payload)

	bytes := []byte{msg.Type}
	bytes = binary.BigEndian.AppendUint16(bytes, uint16(length))
	bytes = append(bytes, payload...)

	if err := sendAll(conn, bytes); err != nil {
		return err
	}

	return nil
}

func Recv(conn net.Conn) (Message, error) {
	msgTypeBytes, err := recvAll(conn, 1)
	if err != nil {
		return Message{}, err
	}
	msgType := msgTypeBytes[0]

	lengthBytes, err := recvAll(conn, 2)
	if err != nil {
		return Message{}, err
	}
	length := int(binary.BigEndian.Uint16(lengthBytes))

	payloadBytes, err := recvAll(conn, length)
	if err != nil {
		return Message{}, err
	}
	payload := string(payloadBytes)

	return Message{
		Type:    msgType,
		Payload: payload,
	}, nil
}

func sendAll(conn net.Conn, bytes []byte) error {
	totalLength := len(bytes)
	totalSent := 0
	for totalSent < totalLength {
		sent, err := conn.Write(bytes[totalSent:])
		if err != nil {
			return err
		}
		totalSent += sent
	}

	return nil
}

func recvAll(conn net.Conn, length int) ([]byte, error) {
	bytes := make([]byte, length)
	totalRead := 0
	for totalRead < length {
		read, err := conn.Read(bytes)
		if err != nil {
			return nil, err
		}
		totalRead += read
	}
	return bytes, nil
}
