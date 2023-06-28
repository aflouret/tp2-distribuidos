# noinspection PyUnresolvedReferences
from health_checker import HealthChecker

import logging
import os

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

TARGETS = os.getenv("TARGETS")
targets = TARGETS.split(",")
logging.info(f"Targets: {targets}")


def main():

    checker = HealthChecker(targets)
    checker.run()


if __name__ == '__main__':
    main()
