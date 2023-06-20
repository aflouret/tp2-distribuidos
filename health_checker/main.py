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
BASE_NAME="checker-"
HOSTNAME= "checker-"+str(NODE_ID)

peers = [ BASE_NAME+str(i) for i in range(GROUP_SIZE) if not i == NODE_ID ]

logging.info(f"Peers: {peers}")

def main():

    checker = Worker()
    group = LeaderGroup(checker, NODE_ID, GROUP_SIZE,HOSTNAME, peers)
    group.run()


if __name__ == '__main__':
    main()
