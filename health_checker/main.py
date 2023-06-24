from leader_election.leader_group import LeaderGroup, Worker
from health_checker import HealthChecker
from replier import CheckerReplier
import logging
import os

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)


HOSTNAME = os.getenv("HOSTNAME")
PEERS = os.getenv("PEERS").split(",")
TARGETS = os.getenv("TARGETS").split(",")

peers = [x for x in PEERS if not x == HOSTNAME]
targets = [x for x in TARGETS if not x == HOSTNAME]


class CheckerWorker(Worker):

    def __init__(self):
        self.checker = HealthChecker(targets)

    def run(self):
        self.checker.run()

    def stop(self):
        self.checker.stop()


def main():

    logging.info(f"Instance hostname: {HOSTNAME}")
    logging.info(f"Peers: {peers}")
    logging.info(f"Targets: {targets}")

    # Start replier
    replier = CheckerReplier()
    replier.run()

    checker = CheckerWorker()
    group = LeaderGroup(checker, 0, len(PEERS), HOSTNAME, peers)
    group.run()


if __name__ == '__main__':
    main()
