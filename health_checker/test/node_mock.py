import logging as log
import socket
# noinspection PyUnresolvedReferences
from messaging_protocol import send, receive, Packet

log.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="DEBUG",
    datefmt='%Y-%m-%d %H:%M:%S',
)

PORT = 12345
OP_CODE_ERORR = -1
OP_CODE_DISCONECTED = 0
OP_CODE_PING = 1
OP_CODE_PONG = 2


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:

    addr = ("", PORT)
    log.info(f"Binding to {addr}")
    s.bind(addr)
    s.listen(1)

    while True:

        log.info("Waiting for checker")
        client, addr = s.accept()
        log.info("Connected!")

        while True:

            try:

                log.info("Waiting for PING")
                packet = receive(client)

                if packet.opcode == OP_CODE_PING:

                    log.info("Received PING!")
                    send(client, Packet.new(OP_CODE_PONG,""))
                    log.info("PONG!")

                else:
                    log.error(f"Unexpected response: {packet.__dict__}")
                    break

            except Exception as e:
                log.error(f"Exception: {e}")
                break

        log.info("Disconected.")
        client.close()
