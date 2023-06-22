# noinspection PyUnresolvedReferences
from time import sleep

from leader_election.leader_group import LeaderGroup, Worker
import logging
import os

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)


NODE_ID = int(os.getenv("NODE_ID"))
GROUP_SIZE = int(os.getenv("GROUP_SIZE"))
BASE_NAME="bully-"
HOSTNAME= "bully-"+str(NODE_ID)

peers = [ BASE_NAME+str(i) for i in range(GROUP_SIZE) if not i == NODE_ID ]

logging.info(f"Peers: {peers}")


class MockWorker(Worker):

    HOSTNAME = HOSTNAME

    def run(self):

        with open("/app/leaders.txt", "a") as file:
            file.write(self.HOSTNAME + "\n")
        sleep(10)

def main():

    checker = MockWorker()
    group = LeaderGroup(checker, NODE_ID, GROUP_SIZE, HOSTNAME, peers)
    group.run()


if __name__ == '__main__':
    main()
