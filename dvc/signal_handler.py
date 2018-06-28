import signal
import threading

from dvc.logger import Logger


class SignalHandler(object):
    def __enter__(self):
        if isinstance(threading.current_thread(), threading._MainThread):
            self.old_handler = signal.signal(signal.SIGINT, self.handler)

    def handler(self, sig, frame):
        Logger.debug('Ignoring SIGINT during critical parts of code...')

    def __exit__(self, type, value, traceback):
        if isinstance(threading.current_thread(), threading._MainThread):
            signal.signal(signal.SIGINT, self.old_handler)
