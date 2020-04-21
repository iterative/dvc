import os
import yaml
import logging

from voluptuous import Schema, Required, Invalid

from dvc.utils.fs import makedirs
from dvc.utils import relpath, dict_sha256

logger = logging.getLogger(__name__)

SCHEMA = Schema(
    {
        Required("cmd"): str,
        Required("deps"): {str: str},
        Required("outs"): {str: str},
    }
)


def _get_cache_hash(cache, key=False):
    return dict_sha256(
        {
            "cmd": cache["cmd"],
            "deps": cache["deps"],
            "outs": list(cache["outs"].keys()) if key else cache["outs"],
        }
    )


def _get_stage_hash(stage):
    if not stage.cmd or not stage.deps or not stage.outs:
        return None

    for dep in stage.deps:
        if dep.scheme != "local" or not dep.def_path or not dep.get_checksum():
            return None

    for out in stage.outs:
        if out.scheme != "local" or not out.def_path or out.persist:
            return None

    return _get_cache_hash(_create_cache(stage), key=True)


def _create_cache(stage):
    return {
        "cmd": stage.cmd,
        "deps": {dep.def_path: dep.get_checksum() for dep in stage.deps},
        "outs": {out.def_path: out.get_checksum() for out in stage.outs},
    }


class StageCache:
    def __init__(self, cache_dir):
        self.cache_dir = os.path.join(cache_dir, "stages")

    def _get_cache_dir(self, key):
        return os.path.join(self.cache_dir, key[:2], key)

    def _get_cache_path(self, key, value):
        return os.path.join(self._get_cache_dir(key), value)

    def _load_cache(self, key, value):
        path = self._get_cache_path(key, value)

        try:
            with open(path, "r") as fobj:
                return SCHEMA(yaml.safe_load(fobj))
        except FileNotFoundError:
            return None
        except (yaml.error.YAMLError, Invalid):
            logger.warning("corrupted cache file '%s'.", relpath(path))
            os.unlink(path)
            return None

    def _load(self, stage):
        key = _get_stage_hash(stage)
        if not key:
            return None

        cache_dir = self._get_cache_dir(key)
        if not os.path.exists(cache_dir):
            return None

        for value in os.listdir(cache_dir):
            cache = self._load_cache(key, value)
            if cache:
                return cache

        return None

    def save(self, stage):
        cache_key = _get_stage_hash(stage)
        if not cache_key:
            return

        cache = _create_cache(stage)
        cache_value = _get_cache_hash(cache)

        if self._load_cache(cache_key, cache_value):
            return

        # sanity check
        SCHEMA(cache)

        path = self._get_cache_path(cache_key, cache_value)
        dpath = os.path.dirname(path)
        makedirs(dpath, exist_ok=True)
        with open(path, "w+") as fobj:
            yaml.dump(cache, fobj)

    def restore(self, stage):
        cache = self._load(stage)
        if not cache:
            return

        deps = {dep.def_path: dep for dep in stage.deps}
        for def_path, checksum in cache["deps"].items():
            deps[def_path].checksum = checksum

        outs = {out.def_path: out for out in stage.outs}
        for def_path, checksum in cache["outs"].items():
            outs[def_path].checksum = checksum
