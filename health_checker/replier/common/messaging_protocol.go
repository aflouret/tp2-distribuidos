package common

import (
	"encoding/binary"
	"errors"
	"net"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

// Config
const (
	opcodeBytes     = 1
	dataLengthBytes = 4
	headerBytes     = opcodeBytes + dataLengthBytes
	MaxPacketSize   = 8192
)

// Encoders and decoders

func encode(data interface{}) []byte {
	switch data.(type) {
	case int:
		return []byte(strconv.Itoa(data.(int)))
	case string:
		return []byte(data.(string))
	case []string:
		return []byte(strings.Join(data.([]string), "#") + "#")
	default:
		return []byte{}
	}
}

func decode(data []byte) interface{} {
	decoded := string(data)
	if n, err := strconv.Atoi(decoded); err == nil {
		return n
	}
	if strings.Contains(decoded, "#") {
		return strings.Split(decoded, "#")[:len(strings.Split(decoded, "#"))-1]
	}
	return decoded
}

// Application layer data packet based on TLV format: Type - Lenght - Value

type Packet struct {
	opcode     int
	dataLength int
	data       []byte
}

func NewPacket(opcode int, data interface{}) *Packet {
	encoded := encode(data)
	return &Packet{opcode, len(encoded), encoded}
}

func (p *Packet) get() interface{} {
	return decode(p.data)
}

// Implementation of the TLV messaging protocol over TCP sockets.

// Upper layer

func Receive(conn net.Conn) (*Packet, error) {

	encodedHeader, err := receive(conn, headerBytes)
	if err != nil {
		return nil, err
	}
	opcode := int(uint8(encodedHeader[0]))
	dataLength := int(binary.BigEndian.Uint32(encodedHeader[opcodeBytes:]))

	buffer := []byte{}
	for len(buffer) < dataLength {

		to_read := getMin(dataLength-len(buffer), MaxPacketSize)
		partial_data, err := receive(conn, to_read)
		if err != nil {
			return nil, err
		}
		buffer = append(buffer, partial_data...)
	}

	return &Packet{opcode, dataLength, buffer}, nil
}

func Send(conn net.Conn, packet *Packet) error {

	encodedHeader := make([]byte, headerBytes)
	encodedHeader[0] = byte(packet.opcode)
	binary.BigEndian.PutUint32(encodedHeader[1:], uint32(packet.dataLength))

	if _, err := send(conn, encodedHeader); err != nil {
		return err
	}

	i, offset := 0, 0
	total_sent := 0

	for total_sent < packet.dataLength {

		i, offset = i+offset, getMin(packet.dataLength-total_sent, MaxPacketSize)
		sent, err := send(conn, packet.data[i:i+offset])
		if err != nil {
			return err
		}

		total_sent += sent
	}

	return nil
}

// Lower layer

func receive(conn net.Conn, bytes_to_read int) ([]byte, error) {

	data := []byte{}
	total_read := 0

	for total_read < bytes_to_read {

		buffer := make([]byte, bytes_to_read-total_read)
		actual_read, err := conn.Read(buffer)
		if err != nil {
			return nil, err
		}

		if actual_read == 0 {
			return nil, errors.New("Short Read")
		}

		total_read += actual_read
		data = append(data, buffer...)
	}

	log.Debugf("action: __receive | result: success | n: %v | data: %s", len(data), data)

	return data, nil
}

func send(conn net.Conn, buffer []byte) (int, error) {

	total_wrote := 0

	for total_wrote < len(buffer) {

		actual_wrote, err := conn.Write(buffer[total_wrote:])
		if err != nil {
			return -1, err
		}

		if actual_wrote == 0 {
			return -1, errors.New("Short write")
		}

		total_wrote += actual_wrote
	}

	log.Debugf("action: __send | result: success | n: %v | data: %s", total_wrote, buffer)

	return total_wrote, nil
}

// Aux

func getMin(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
