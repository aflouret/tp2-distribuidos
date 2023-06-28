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
PORT = 12300

# Leader election messages
PING = 'PING'
ELECTION_MSG = 'ELECTION'
IMALIVE_MSG = 'IMALIVE'
COORDINATOR_MSG = 'COORDINATOR'
OK_CORDINATOR_MSG = 'OK'
IGNORING='IGNORING'

mutex = threading.Lock()


class Worker:

    def run(self):
        pass

    def stop(self):
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
                self.spawn_worker()

    def spawn_worker(self):

        log.info(f"Bully Election | Start Working.")

        # Spawn worker and stay pinging to other nodes of the group
        w = threading.Thread(target=self.worker.run, daemon=False)
        w.start()

        timer = Timer()

        while self.running and self.leader == self.id:
            for idx, data in self.group.items():
                if idx == self.id: continue
                log.debug(f"Bully Election | Ping to: {idx}")
                self.send(data["addr"], PING)
            timer.start(PING_INTERVAL)

        self.worker.stop()
        w.join()

        log.info(f"Bully Election | Stop working.")

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

            addr, msg = self.receive()

            node_id = msg[0]
            msg_id = msg[1]
            msd_code = msg[2]
            param = msg[3]

            # Filter and sequence control
            last_msg = self.group[node_id]["last_msg_id"]
            if node_id not in self.group or last_msg > int(msg_id):
                self.send(addr[0], IGNORING, last_msg)
                continue

            self.group[node_id]["last_msg_id"] = int(msg_id)

            # Handle messages
            if msd_code == ELECTION_MSG:
                log.debug(f"Bully Election | New election detected.. {node_id}")
                self.send(addr[0], IMALIVE_MSG)

            if msd_code == IMALIVE_MSG:
                self.group[node_id]["alive"] = True

            if msd_code == COORDINATOR_MSG:
                log.info(f"Bully Election | New lider detected.. {node_id}")
                self.send(addr[0], OK_CORDINATOR_MSG)
                self.leader = node_id

            if msd_code == OK_CORDINATOR_MSG:
                self.group[node_id]["coordinated"] = True

            if msd_code == PING:

                # Si soy el lider y me mandan un ping, se rompe la invarianza de unico lider.
                if self.leader == self.id:

                    log.info(f"Bully Election | Detected other leader alive: {node_id}.")

                    # Sobrevive el lider de mayor ID
                    if self.id < node_id:
                        log.info(f"Abandoning..")
                        self.leader = node_id

                    else:
                        log.info(f"Ignoring..")
                        continue

                log.debug(f"Bully Election | Ping received! resetting timer...")
                self.iddle_stage.reset()

            if msd_code == IGNORING:

                # Permite recuperar su id_msg ante una caida.
                if int(param) > self.id_msg:
                    with mutex:
                        self.id_msg = int(param)

    def send(self, addr, msg, param=""):

        log.debug(f"Bully Election | Sending message to {addr} >> {msg}")

        with mutex:
            msg = self.id + "." + str(self.id_msg) + "." + msg + "." + str(param)
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

