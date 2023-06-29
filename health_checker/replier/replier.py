import socket
import threading
import logging as log
from messaging_protocol import Packet,send,receive

PORT = 12345
OP_CODE_ERORR = -1
OP_CODE_DISCONECTED = 0
OP_CODE_PING = 1
OP_CODE_PONG = 2


class CheckerReplier:

    def __init__(self):
        self._server_socket = None
        self.listening = False
        self.t = None

    def run(self):

        self.listening = True
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', PORT))
        self._server_socket.listen(1)
        self.t = threading.Thread(target=self.listen_loop)
        self.t.start()

    def stop(self):
        self.listening = False

    def listen_loop(self):

        log.info(f"Replier | listening on port {PORT}")

        while self.listening:

            checker, addr = self._server_socket.accept()

            log.info(f"Replier | New connection from: {addr}")
            self.replier(checker)
            checker.close()

    def replier(self, checker):

        log.debug("Replier | Waiting for PING")
        while self.listening:

            try:

                packet = receive(checker)

                if packet.opcode == OP_CODE_PING:

                    log.info("Replier | Received PING!")
                    send(checker, Packet.new(OP_CODE_PONG, ""))
                    log.info("Replier | Replied PONG!")

                else:
                    log.error(f"Replier | Unexpected opcode response: {packet.opcode}")
                    break

            except Exception as e:
                log.error(f"Replier | Exception: {e}")
                break
