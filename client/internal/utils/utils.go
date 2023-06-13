package utils

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"tp1/common/protocol"
)

func ReadBatch(scanner *bufio.Scanner, size int) (string, error) {
	var batch string

	for i := 0; i < size; i++ {
		if scanner.Scan() {
			line := scanner.Text()
			delimiter := ";"
			batch += line + delimiter
		} else {
			break
		}
	}
	batch = strings.TrimSuffix(batch, ";")
	return batch, scanner.Err()
}

func SendControlMessage(conn net.Conn, msgType uint8, payload string) error {
	msg := protocol.NewControlMessage(msgType, payload)
	if err := protocol.Send(conn, msg); err != nil {
		return err
	}
	ackMessage, err := protocol.Recv(conn)
	if err != nil {
		return err
	}
	if ackMessage.Type != protocol.Ack {
		return fmt.Errorf("received wrong message")
	}
	return nil
}

func SendDataMessage(conn net.Conn, payload string) error {
	msg := protocol.NewDataMessage(payload)
	err := protocol.Send(conn, msg)
	if err != nil {
		return err
	}
	ackMessage, err := protocol.Recv(conn)
	if err != nil {
		return err
	}
	if ackMessage.Type != protocol.Ack {
		return fmt.Errorf("received wrong message")
	}

	return nil
}
