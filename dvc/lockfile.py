import json

from typing import TYPE_CHECKING
from collections import OrderedDict

from dvc.exceptions import DvcException
from dvc.schema import COMPILED_LOCKFILE_SCHEMA
from voluptuous import MultipleInvalid

if TYPE_CHECKING:
    from dvc.repo import Repo


class LockfileCorruptedError(DvcException):
    def __init__(self, path):
        super().__init__("Lockfile '{}' is corrupted.".format(path))


def exists(repo: "Repo", path: str) -> bool:
    return repo.tree.exists(path)


def read(repo: "Repo", path: str) -> dict:
    with repo.tree.open(path) as f:
        return json.load(f, object_pairs_hook=OrderedDict)


def write(repo: "Repo", path: str, data: dict) -> None:
    with repo.tree.open(path, "w+") as f:
        json.dump(data, f)


def load(repo: "Repo", path: str) -> dict:
    if not exists(repo, path):
        return {}
    try:
        return COMPILED_LOCKFILE_SCHEMA(read(repo, path))
    except MultipleInvalid:
        raise LockfileCorruptedError(path)


def dump(repo: "Repo", path: str, stage_data: dict):
    if not exists(repo, path):
        data = stage_data
    else:
        data = read(repo, path)
        data.update(stage_data)

    write(repo, path, COMPILED_LOCKFILE_SCHEMA(data))
