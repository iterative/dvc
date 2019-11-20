"""
Interact with the user's ID stored under the global config directory.

The file should contain a JSON with a "user_id" key:

    {"user_id": "16fd2706-8baf-433b-82eb-8c7fada847da"}

IDs are generated randomly with UUID.
"""

import json
import logging
import uuid

from dvc.config import Config
from dvc.lock import Lock, LockError
from dvc.utils import makedirs


logger = logging.getLogger(__name__)

config_dir = Config.get_global_config_dir()
fname = config_dir / "user_id"


def find_or_create():
    lockfile = fname.with_suffix(".lock")

    try:
        with Lock(lockfile):
            return _load() or _create()
    except LockError:
        logger.debug("Failed to acquire {lock}".format(lockfile))


def _load():
    if not fname.exists:
        return None

    with open(fname) as fobj:
        try:
            return json.load(fobj).get("user_id")
        except json.JSONDecodeError:
            pass


def _create():
    user_id = str(uuid.uuid4())

    makedirs(fname.parent, exist_ok=True)

    with open(fname, "w") as fobj:
        json.dump({"user_id": user_id}, fobj)

    return user_id
