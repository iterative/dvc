import functools
import os
import threading
import logging

from dvc.progress import Tqdm


LOGGER = logging.getLogger(__name__)


MIME_GOOGLE_APPS_FOLDER = "application/vnd.google-apps.folder"


class TrackFileReadProgress(object):
    UPDATE_AFTER_READ_COUNT = 30

    def __init__(self, progress_name, fobj):
        self.progress_name = progress_name
        self.fobj = fobj
        self.file_size = os.fstat(fobj.fileno()).st_size
        self.tqdm = Tqdm(desc=self.progress_name, total=self.file_size)
        self.update_counter = 0

    def read(self, size):
        if self.update_counter == 0:
            self.tqdm.update_to(self.fobj.tell())
            self.update_counter = self.UPDATE_AFTER_READ_COUNT
        else:
            self.update_counter -= 1
        return self.fobj.read(size)

    def close(self):
        self.fobj.close()
        self.tqdm.close()

    def __getattr__(self, attr):
        return getattr(self.fobj, attr)


def only_once(func):
    lock = threading.Lock()
    locks = {}
    results = {}

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        key = (args, tuple(kwargs.items()))
        # could do with just setdefault, but it would require
        # create/delete a "default" Lock() object for each call, so it
        # is better to lock a single one for a short time
        with lock:
            if key not in locks:
                locks[key] = threading.Lock()
        with locks[key]:
            if key not in results:
                results[key] = func(*args, **kwargs)
        return results[key]

    return wrapped


@only_once
def shared_token_warning():
    LOGGER.warning(
        "Warning: a shared GoogleAPI token is in use. "
        "Please create your own token."
    )
