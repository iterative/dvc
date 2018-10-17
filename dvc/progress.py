from __future__ import print_function
import sys
import threading


class Progress(object):
    """
    Simple multi-target progress bar.
    """
    def __init__(self):
        self._n_total = 0
        self._n_finished = 0
        self._lock = threading.Lock()
        self._line = None

    def set_n_total(self, total):
        self._n_total = total
        self._n_finished = 0

    @property
    def is_finished(self):
        return self._n_total == self._n_finished

    def _clearln(self):
        print('\r\x1b[K', end='')

    def _writeln(self, line):
        self._clearln()
        print(line, end='')
        sys.stdout.flush()

    def refresh(self, line=None):
        # Just go away if it is locked. Will update next time
        if not self._lock.acquire(False):
            return

        if line is None:
            line = self._line

        if sys.stdout.isatty() and line is not None:
            self._writeln(line)
            self._line = line

        self._lock.release()

    def update_target(self, name, current, total):
        self.refresh(self._bar(name, current, total))

    def finish_target(self, name):
        # We have to write a msg about finished target
        with self._lock:
            bar = self._bar(name, 100, 100)

            if sys.stdout.isatty():
                self._clearln()

            print(bar)

            self._n_finished += 1
            self._line = None

    def _bar(self, target_name, current, total):
        """
        Make a progress bar out of info, which looks like:
        (1/2): [########################################] 100% master.zip
        """
        bar_len = 30

        if total is None:
            progress = 0
            percent = "?% "
        else:
            total = int(total)
            progress = int((100 * current)/total) if current < total else 100
            percent = str(progress) + "% "

        if self._n_total > 1:
            num = "({}/{}): ".format(self._n_finished + 1, self._n_total)
        else:
            num = ""

        n_sh = int((progress * bar_len)/100)
        n_sp = bar_len - n_sh
        bar = "[" + '#'*n_sh + ' '*n_sp + "] "

        return num + bar + percent + target_name

    def __enter__(self):
        self._lock.acquire(True)
        if self._line is not None:
            self._clearln()

    def __exit__(self, type, value, tb):
        if self._line is not None:
            self.refresh()
        self._lock.release()


progress = Progress()
