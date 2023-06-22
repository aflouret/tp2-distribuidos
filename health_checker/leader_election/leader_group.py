from time import sleep

from . import retry
from .timer import Timer
import threading
import socket
import logging as log

# Leader election params
PING_INTERVAL = 2
TIMEOUT_LEADER = 10
TIMEOUT_ELECTION = 5
TIMEOUT_COORDINATION = 10
RETRIES = 3

MSG_SIZE = 128
PORT = 12345

# Leader election messages
PING = 'PING'
ELECTION_MSG = 'ELECTION'
IMALIVE_MSG = 'IMALIVE'
COORDINATOR_MSG = 'COORDINATOR'
OK_CORDINATOR_MSG = 'OK'

mutex = threading.Lock()


class Worker:

    def run(self):
        pass


class LeaderGroup:

    def __init__(self, worker: Worker, id_node: int, group_size: int, addr, hosts):

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        log.debug(f"Bully Election | Binding to {(addr, PORT)}")
        self.sock.bind((addr, PORT))
        self.worker = worker
        self.id = addr
        self.group_size = group_size
        self.group = {}
        self.running = True
        self.leader = -1
        self.iddle_stage = Timer()
        self.id_msg = 0

        for name in hosts:
            self.group[name] = {"addr": name, "last_msg_id": 0, "alive": False, "coordinated": False}

    def run(self):

        t = threading.Thread(target=self.listen_messages, daemon=False)
        t.start()

        log.info(f"Bully Election | Starting node {self.id} in group of {self.group_size} members.")

        while self.running:

            # Si el lider manda ping frecuentemente, nunca debería salir de aca
            self.iddle_stage.start(TIMEOUT_LEADER)
            self.leader = -1

            log.info(f"Bully Election | Leader fallen. Starting election.")
            self.start_election()

            log.info(f"Bully Election | Leader chosen: {self.leader}")
            if self.leader == self.id:
                pinging = threading.Thread(target=self.pinging, daemon=False)
                pinging.start()
                self.worker.run()
                self.shutdown()

    def pinging(self):

        timer = Timer()
        while self.running:
            for idx, data in self.group.items():
                if idx == self.id: continue
                log.debug(f"Bully Election | Ping to: {idx}")
                self.send(data["addr"], PING)
            timer.start(PING_INTERVAL)

    def start_election(self):

        with mutex:
            for _, data in self.group.items():
                data["alive"] = False
                data["coordinated"] = False

        has_answer = self.election()

        if not has_answer:
            log.info(f"Bully Election | Election got no answer.")
            # Auto proclamarse lider
            self.coordinate()
            self.leader = self.id

        else:
            log.info(f"Bully Election | Election was replied. Waiting for coordination..")
            coordination_stage = Timer()
            coordination_stage.start(TIMEOUT_COORDINATION)

    @retry(RETRIES)
    def election(self):

        election_stage = Timer()

        for idx, node in self.group.items():

            if idx <= self.id:
                continue

            if node["alive"]:
                return True

            log.debug(f"Bully Election | Sending election to {idx}")
            self.send(node["addr"], ELECTION_MSG)

        election_stage.start(TIMEOUT_ELECTION/RETRIES)

    @retry(RETRIES)
    def coordinate(self):

        coordination_stage = Timer()
        all_coordinated = True

        for idx, node in self.group.copy().items():

            if idx == self.id:
                continue

            all_coordinated &= node["coordinated"]
            if node["coordinated"]:
                continue

            log.debug(f"Bully Election | Sending coordination to {idx}")
            self.send(node["addr"], COORDINATOR_MSG)

        coordination_stage.start(TIMEOUT_COORDINATION/RETRIES)

        if all_coordinated:
            return True

    def listen_messages(self):

        # Listen mesages from all instances

        while self.running:

            addr, (node_id, msg_id, msg) = self.receive()

            # Filter and sequence control
            if node_id not in self.group or self.group[node_id]["last_msg_id"] > int(msg_id):
                continue

            self.group[node_id]["last_msg_id"] = int(msg_id)

            # Handle messages
            if msg == ELECTION_MSG:
                log.debug(f"Bully Election | New election detected.. {node_id}")
                self.send(addr[0], IMALIVE_MSG)

            if msg == IMALIVE_MSG:
                self.group[node_id]["alive"] = True

            if msg == COORDINATOR_MSG:
                log.info(f"Bully Election | New lider detected.. {node_id}")
                self.send(addr[0], OK_CORDINATOR_MSG)
                self.leader = node_id

            if msg == OK_CORDINATOR_MSG:
                self.group[node_id]["coordinated"] = True

            if msg == PING:
                log.debug(f"Bully Election | Ping received! resetting timer...")
                self.iddle_stage.reset()

    def send(self, addr, msg):

        log.debug(f"Bully Election | Sending message to {addr} >> {msg}")

        with mutex:
            msg = self.id + "." + str(self.id_msg) + "." + msg
            msg = msg.ljust(MSG_SIZE).encode()

            try:
                self.sock.sendto(msg, (addr, PORT))
            except socket.gaierror as e:
                log.error(f"Bully Election | Error sending to {addr}. Maybe host is down and has no IP.")
            self.id_msg += 1

    def receive(self):

        data, addr = self.sock.recvfrom(MSG_SIZE)

        log.debug(f"Bully Election | Received message: {data}")

        return addr, data.rstrip().decode().split(".")

    def shutdown(self):
        self.running = False
        self.sock.close()

