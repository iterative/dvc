import os
import json
import hashlib

from dvc.utils.fs import makedirs


def _sha256(string):
    return hashlib.sha256(string.encode()).hexdigest()


def _get_hash(stage):
    if not stage.cmd or not stage.deps or not stage.outs:
        return None

    string = _sha256(stage.cmd)
    for dep in stage.deps:
        if not dep.def_path or not dep.get_checksum():
            return None

        string += _sha256(dep.def_path)
        string += _sha256(dep.get_checksum())

    for out in stage.outs:
        if not out.def_path or out.persist:
            return None

        string += _sha256(out.def_path)

    return _sha256(string)


def _get_cache(stage):
    return {
        "cmd": stage.cmd,
        "deps": {dep.def_path: dep.get_checksum() for dep in stage.deps},
        "outs": {out.def_path: out.get_checksum() for out in stage.outs},
    }


def _get_cache_path(stage):
    sha = _get_hash(stage)
    if not sha:
        return None

    cache_dir = os.path.join(stage.repo.cache.local.cache_dir, "stages")

    return os.path.join(cache_dir, sha[:2], sha)


def save(stage):
    path = _get_cache_path(stage)
    if not path or os.path.exists(path):
        return

    dpath = os.path.dirname(path)
    makedirs(dpath, exist_ok=True)
    with open(path, "w+") as fobj:
        json.dump(_get_cache(stage), fobj)


def restore(stage):
    path = _get_cache_path(stage)
    if not path or not os.path.exists(path):
        return

    with open(path, "r") as fobj:
        cache = json.load(fobj)

    outs = {out.def_path: out for out in stage.outs}
    for def_path, checksum in cache["outs"].items():
        outs[def_path].checksum = checksum

    for dep in stage.deps:
        dep.save()
