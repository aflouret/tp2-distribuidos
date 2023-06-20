import time
from time import sleep
import threading

mutex = threading.Lock()


class Timer:

    def __init__(self):
        self.resetted_at = 0

    def reset(self):

        with mutex:
            self.resetted_at = time.time()

    def start(self, seconds):

        remaining = seconds
        while remaining > 0:
            sleep(remaining)
            now = time.time()
            with mutex:
                remaining = max(0, self.resetted_at + seconds - now)
