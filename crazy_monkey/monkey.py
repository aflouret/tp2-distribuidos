
from random import random
from time import sleep
import docker

client = docker.from_env()


class CrazyMonkey:

    def __init__(self, targets):
        self.ignore_list = []
        self.targets = targets

    def stop_riot(self, *args):
        print("aaaa",flush=True)
        self.riot_mode_status = False

    def attack(self, name=None):

        if not name:
            name = self.__random_target()

        if name not in self.targets:
            print(f"CrazyMonkey | {name} not in target list.")
            return

        self.__attack(name)

    def __random_target(self):
        random_idx = int(random()*len(self.targets))
        return self.targets[random_idx]

    def __attack(self, name):

        try:
            container = client.containers.get(name)
            container.stop(timeout=1)
            print(f"CrazyMonkey | {name} was knocked down!")

        except Exception as e:
            print(f"CrazyMonkey | Error attempting to attack {name}: {e}")

    def riot_mode(self, repeats=10, interval=10):

        print(f"CrazyMonkey | Starting riot with  {repeats} repeats with interval of {interval} seconds.")
        try:
            for _ in range(repeats):
                self.__attack(self.__random_target())
                sleep(interval)

        except KeyboardInterrupt:
            print(f" CrazyMonkey | Peace has returned to the system :)")

    def attack_stage(self, stage):
        pass

    def ignore(self, name):
        if name in self.targets:
            self.ignore_list.append(name)
            self.targets.remove(name)
            print(f"CrazyMonkey | Ignored {name}.")

    def remove_ignore(self, name):
        if name in self.ignore:
            self.targets.append(name)
            self.ignore_list.remove(name)
            print(f"CrazyMonkey | Removed {name} from ignore list.")

    def clear(self):
        self.targets.extend(self.ignore_list)
        self.ignore_list.clear()
