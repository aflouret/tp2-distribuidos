import socket
import threading
from time import sleep
from messaging_protocol import send, receive, Packet
import logging as log

import docker
client = docker.from_env()

CHECKER_PORT = 12345
MSG_SIZE = 128
CHECKING_INTERVAL = 10
LUTO_TIME = 10
CONNECTION_RETRIES = 3
CONNETION_RETRY_BASE_TIME = 1
SOCKET_TIMEOUT = 20

OP_CODE_ERORR = -1
OP_CODE_DISCONECTED = 0
OP_CODE_PING = 1
OP_CODE_PONG = 2

# Errors that are considered as a failure indicator
TARGET_ERRORS = (ConnectionError, TimeoutError, socket.gaierror)


class HealthChecker:

    def __init__(self, targets):
        self.targets = targets
        self.running = True

    def run(self):

        thread_poll = []

        # Connect stage
        for hostname in self.targets:
            t = threading.Thread(target=self.handle_node, args=[hostname])
            t.start()
            thread_poll.append(t)

        for t in thread_poll:
            t.join()

    def handle_node(self, hostname):

        s = self.connect_to(hostname)
        while self.running:

            success = s and self.do_ping(s, hostname)

            if not success:
                s = self.bring_to_live(hostname)

            sleep(CHECKING_INTERVAL)

    def bring_to_live(self, name):

        container = client.containers.get(name)
        container.restart(timeout=LUTO_TIME)
        sleep(LUTO_TIME)

        return self.connect_to(name)

    def do_ping(self, s, hostname):

        log.info(f"Pinging to {hostname}")

        success = False

        try:
            ping = Packet.new(OP_CODE_PING, "")
            send(s, ping)

            response = receive(s).opcode
            success = response == OP_CODE_PONG

            if not success:
                log.error(f"Host {hostname} disconected. Reason: Unexpected opcode response ({response}).")

        except TARGET_ERRORS as e:
            log.error(f"Host {hostname} disconected. Reason: {e} ")

        if not success:
            s.close()

        return success

    def connect_to(self, hostname):

        s = None

        if hostname not in self.targets:
            log.error(f"Unexpected host: {hostname}")
            return s

        for i in range(CONNECTION_RETRIES):

            connected = False
            try:
                addr = (hostname, CHECKER_PORT)
                log.debug(f"{addr} -> Trying connection.")

                s = self.__new_sock()
                s.settimeout(SOCKET_TIMEOUT)
                s.connect(addr)

                log.info(f"{addr} -> Connected!")
                connected = True

            except TARGET_ERRORS as e:
                log.error(f"Connection error of host {hostname}: {e} ")
                s.close()
                s = None

            if connected:
                return s

            sleep(CONNETION_RETRY_BASE_TIME*(2**i))

        log.error(f"Unable to connect to {hostname}")

        return s

    def kill(self, target):


        log.info(f"[EVENT] - Killed {target}")

    def revive(self, target):
        client.containers.run(target, detach=True)

    def __new_sock(self):
        return socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Agregar timeout al socket
# Handlear una desconección inicial
