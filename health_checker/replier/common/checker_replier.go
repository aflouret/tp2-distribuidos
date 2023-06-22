package common

import (
	"errors"
	"fmt"
	"net"
)

const (
	PORT                = 12345
	OP_CODE_ERORR       = -1
	OP_CODE_DISCONECTED = 0
	OP_CODE_PING        = 1
	OP_CODE_PONG        = 2
)

type HealhCheckerReplier struct {
	listener net.Listener
	checker  net.Conn
	running  bool
}

func NewReplier() *HealhCheckerReplier {

	replier := &HealhCheckerReplier{
		listener: nil,
		running:  false,
	}

	return replier
}

func (hcr *HealhCheckerReplier) Run() error {

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", PORT))
	if err != nil {
		return err
	}
	hcr.listener = listener
	hcr.running = true

	go hcr.acceptLoop()

	return nil
}

func (hcr *HealhCheckerReplier) Stop() error {

	hcr.running = false

	if hcr.checker != nil {
		err := hcr.checker.Close()
		if err != nil {
			return err
		}

		hcr.checker = nil
	}

	if hcr.listener != nil {
		err := hcr.listener.Close()
		if err != nil {
			return err
		}
		hcr.listener = nil
	}
	
	return nil
}

func (hcr *HealhCheckerReplier) acceptLoop() {

	fmt.Printf("HealhCheckerReplier listening on port %d\n", PORT)

	for hcr.running {
		conn, err := hcr.listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}
		hcr.checker = conn
		fmt.Printf("New connection from: %v\n", conn.RemoteAddr())

		if shouldExit, _ := hcr.replier(conn); shouldExit {
			break
		}
	}
}

func (hcr *HealhCheckerReplier) replier(conn net.Conn) (bool, error) {

	for hcr.running {

		// Receive ping
		packet, err := Receive(conn)
		if err != nil {
			return false, err
		}

		if packet.opcode != OP_CODE_PING {
			return false, errors.New(fmt.Sprintf("Unexpected opcode response: %v", packet.opcode))
		}

		fmt.Printf("Received Ping!")

		// Send pong
		err = Send(conn, NewPacket(OP_CODE_PONG, ""))
		if err != nil {
			return false, err
		}

		fmt.Printf("Replied Pong!")

	}

	return true, nil
}
