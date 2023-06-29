import os
import signal
from sys import argv

from monkey import CrazyMonkey


def load_targets():
    with open("targets.csv") as f:
        targets = f.read()

    return targets.split(",")


def mainloop(monkey):
    print("Escribe un comando:")
    while True:

        command = input(" > ").split(" ")
        command_base = command[0].lower()

        if command_base == "attack":
            if len(command) == 2:
                monkey.attack(command[1])
            else:
                monkey.attack()

        elif command_base == "riot":
            try:
                if len(command) == 3:
                    monkey.riot_mode(int(command[1]), int(command[2]))
                elif len(command) == 2:
                    monkey.riot_mode(int(command[1]))
                else:
                    monkey.riot_mode()

            except Exception as e:
                print(f"Error: {e}")

        elif command_base == "targets":
            print(f"Targets: {monkey.targets}")
            print(f"Ignored: {monkey.ignore_list}")

        elif command_base == "ignore":
            if len(command) > 1:
                for t in command[1:]:
                    monkey.ignore(t)
            else:
                print("Expected container name.")

        elif command_base == "clear":
            monkey.clear()

        elif command_base == "help":
            print("Commands: operation <mandatory> [optional] ")
            print()
            print("\t- attack [container]           \tAttack random or specified container.")
            print("\t- riot [repeats] [interval]   "
                  "\tAttack random containers [repeats] times each [interval] seconds.")
            print("\t- targets                      \tList of targets.")
            print("\t- ignore <container>           \tIgnore container so it cant be attacked.")
            print("\t- clear                        \tClear the ignore list.")
            print("\t- exit                         \tExit the program")

        elif command[0].lower() == "exit":
            break


def main():
    print("Starting Crazy Monkey")

    targets = load_targets()

    monkey = CrazyMonkey(targets)
    signal.signal(signal.SIGTERM, monkey.stop_riot)
    mainloop(monkey)


if __name__ == '__main__':
    main()
