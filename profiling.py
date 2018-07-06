import logging
import time


class timeit:
    """Helper class to easily report long running processes.
    Usage:

        with timeit("Took a long time", 5000):
            time.sleep(6)

    Or alternatively you can:

        timer = timeit()
        ...
        print(timer.elapsed())
        timer.restart()
        ...
        print(timer.elapsed())
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    def __init__(self, text=None, min_time=None):
        self.text = text
        self.min_time = min_time
        self.start = time.monotonic()

    def restart(self):
        self.start = time.monotonic()
        return self.start

    def elapsed(self):
        return (time.monotonic() - self.start)*1000

    def __enter__(self):
        self.restart()

    def __exit__(self, kind, value, traceback):
        ms = self.elapsed()
        if self.min_time is None or ms > self.min_time:
            self.logger.info(self.text + ': %dms', ms)

    def get_logger(self):
        return self.logger
