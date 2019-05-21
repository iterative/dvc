import functools
import os
import threading
import logging

from dvc.progress import progress


logger = logging.getLogger(__name__)


MIME_GOOGLE_APPS_FOLDER = "application/vnd.google-apps.folder"


class TrackFileReadProgress(object):
    def __init__(self, progress_name, fobj):
        self.progress_name = progress_name
        self.fobj = fobj
        self.file_size = os.fstat(fobj.fileno()).st_size

    def read(self, size):
        progress.update_target(
            self.progress_name, self.fobj.tell(), self.file_size
        )
        return self.fobj.read(size)

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


def response_error_message(response):
    try:
        message = response.json()["error"]["message"]
    except (TypeError, KeyError):
        message = response.text
    return "HTTP {}: {}".format(response.status_code, message)


def response_is_ratelimit(response):
    if response.status_code not in (403, 429):
        return False
    errors = response.json()["error"]["errors"]
    domains = [i["domain"] for i in errors]
    return "usageLimits" in domains


def metadata_isdir(metadata):
    return metadata["mimeType"] == MIME_GOOGLE_APPS_FOLDER


@only_once
def shared_token_warning():
    logger.warning(
        "Warning: a shared GoogleAPI token is in use. "
        "Please create your own token."
    )
