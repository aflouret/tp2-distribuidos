import concurrent
import queue
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from math import ceil
from time import sleep
from messaging_protocol import send, receive, Packet
import logging as log

import docker
client = docker.from_env()

CHECKER_PORT = 12345
MSG_SIZE = 128
CHECKING_INTERVAL = 2
LUTO_TIME = 10
CONNECTION_RETRIES = 3
CONNETION_RETRY_BASE_TIME = 1
SOCKET_TIMEOUT = 30

OP_CODE_ERORR = -1
OP_CODE_DISCONECTED = 0
OP_CODE_PING = 1
OP_CODE_PONG = 2

# Errors that are considered as a failure indicator
TARGET_ERRORS = (ConnectionError, TimeoutError, socket.gaierror, OSError)


class HealthChecker:

    def __init__(self, targets, workers):
        self.targets = targets
        self.running = False
        self.workers = workers
        self.mutex = threading.Lock()
        self.queue = queue.Queue()

        for x in targets:
            self.queue.put((x, None))

    def run(self):

        self.running = True
        thread_pool = []

        # Spawn workers
        for i in range(self.workers):
            t = threading.Thread(target=self.process_tasks)
            t.start()
            thread_pool.append(t)

        for t in thread_pool:
            t.join()

        while not self.queue.empty():
            target, s = self.get_task()
            if s:
                s.close()

    def get_task(self):
        with self.mutex:
            value = self.queue.get()
        return value

    def put_task(self, value):
        with self.mutex:
            self.queue.put(value)

    def process_tasks(self):

        while self.running:

            target, s = None, None

            try:
                target, s = self.get_task()
                if not s:
                    s = self.connect_to(target)

                success = s and self.do_ping(s, target)
                if not success:
                    s = self.bring_to_live(target)
            except Exception as e:
                print(f"HealthChecker | Unexpected error: {e}")

            if target:
                self.put_task((target, s))

            sleep(CHECKING_INTERVAL)

    def bring_to_live(self, name):

        container = client.containers.get(name)
        container.restart(timeout=LUTO_TIME)
        sleep(LUTO_TIME)

        return self.connect_to(name)

    def do_ping(self, s, hostname):

        log.debug(f"HealthChecker | Pinging to {hostname}")

        success = False

        try:
            ping = Packet.new(OP_CODE_PING, "")
            send(s, ping)

            response = receive(s).opcode
            success = response == OP_CODE_PONG

            if not success:
                log.error(f"HealthChecker | Host {hostname} disconected. Reason: Unexpected opcode response ({response}).")

        except TARGET_ERRORS as e:
            log.error(f"HealthChecker | Host {hostname} disconected. Reason: {e} ")

        if not success:
            s.close()

        return success

    def connect_to(self, hostname):

        s = None

        if hostname not in self.targets:
            log.error(f"HealthChecker | Unexpected host: {hostname}")
            return s

        for i in range(CONNECTION_RETRIES):

            connected = False
            try:
                addr = (hostname, CHECKER_PORT)
                log.debug(f"HealthChecker | {addr} -> Trying connection.")

                s = self.__new_sock()
                s.settimeout(SOCKET_TIMEOUT)
                s.connect(addr)

                log.info(f"HealthChecker | {addr} -> Connected!")
                connected = True

            except TARGET_ERRORS as e:
                log.error(f"HealthChecker | Connection error of host {hostname}: {e} ")
                s.close()
                s = None

            if connected:
                return s

            sleep(CONNETION_RETRY_BASE_TIME*(2**i))

        log.error(f"HealthChecker | Unable to connect to {hostname}")

        return s

    def __new_sock(self):
        return socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def stop(self):
        self.running = False
        log.info(f"HealthChecker | Stopping..")
