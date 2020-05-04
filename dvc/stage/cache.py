import os
import yaml
import logging

from voluptuous import Invalid

from dvc.schema import COMPILED_LOCK_FILE_STAGE_SCHEMA
from dvc.serialize import to_single_stage_lockfile
from dvc.stage.loader import StageLoader
from dvc.utils.fs import makedirs
from dvc.utils import relpath, dict_sha256
from dvc.utils.stage import dump_stage_file

logger = logging.getLogger(__name__)


def _get_cache_hash(cache, key=False):
    if key:
        cache["outs"] = [out["path"] for out in cache.get("outs", [])]
    return dict_sha256(cache)


def _get_stage_hash(stage):
    if not (stage.cmd and stage.deps and stage.outs):
        return None

    for dep in stage.deps:
        if not (dep.scheme == "local" and dep.def_path and dep.get_checksum()):
            return None

    for out in stage.outs:
        if out.scheme != "local" or not out.def_path or out.persist:
            return None

    return _get_cache_hash(to_single_stage_lockfile(stage), key=True)


class StageCache:
    def __init__(self, cache_dir):
        self.cache_dir = os.path.join(cache_dir, "runs")

    def _get_cache_dir(self, key):
        return os.path.join(self.cache_dir, key[:2], key)

    def _get_cache_path(self, key, value):
        return os.path.join(self._get_cache_dir(key), value)

    def _load_cache(self, key, value):
        path = self._get_cache_path(key, value)

        try:
            with open(path, "r") as fobj:
                return COMPILED_LOCK_FILE_STAGE_SCHEMA(yaml.safe_load(fobj))
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

        cache = to_single_stage_lockfile(stage)
        cache_value = _get_cache_hash(cache)

        if self._load_cache(cache_key, cache_value):
            return

        # sanity check
        COMPILED_LOCK_FILE_STAGE_SCHEMA(cache)

        path = self._get_cache_path(cache_key, cache_value)
        dpath = os.path.dirname(path)
        makedirs(dpath, exist_ok=True)
        dump_stage_file(path, cache)

    def is_cached(self, stage):
        return bool(self._load(stage))

    def restore(self, stage):
        cache = self._load(stage)
        if not cache:
            return
        StageLoader.fill_from_lock(stage, cache)
